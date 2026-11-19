"""One server for the customer chat and the human-agent console.

Run:   uvicorn server:app --port 8000
Open:  http://localhost:8000/          customer chat
       http://localhost:8000/console   supervisor / human-agent console

The ADK agent runs in-process. This server watches the events it produces and
turns them into work items for humans:

  adk_request_confirmation call     -> approval (tool-level, require_confirmation)
  long-running tool call            -> approval (tool-level, LongRunningFunctionTool)
  transfer_to_human call            -> chat in the human queue (conversation-level)

A human's answer goes back into the same ADK session: a FunctionResponse for
approvals, or session events / state for chat takeover.
"""

import asyncio
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv(Path(__file__).parent / "support_agent" / ".env")

from google.adk.events import Event, EventActions  # noqa: E402
from google.adk.runners import Runner  # noqa: E402
from google.adk.sessions import InMemorySessionService  # noqa: E402
from google.genai import types  # noqa: E402

from support_agent.agent import HANDOFF_REASON, HANDOFF_STATUS, root_agent  # noqa: E402

APP_NAME = "support_agent"
USER_ID = "customer"
CONFIRMATION_CALL = "adk_request_confirmation"
STATIC = Path(__file__).parent / "static"

session_service = InMemorySessionService()
runner = Runner(app_name=APP_NAME, agent=root_agent, session_service=session_service)
app = FastAPI(title="Shopio support")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# --------------------------------------------------------------------- model

@dataclass
class Message:
    role: str  # customer | ai | human | system | approval
    text: str = ""
    author: str = ""
    approval_id: str = ""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])
    ts: float = field(default_factory=time.time)


@dataclass
class Approval:
    id: str
    session_id: str
    customer: str
    kind: str  # confirmation | long_running
    tool: str
    args: dict[str, Any]
    call_id: str  # id of the function call the human's answer must respond to
    status: str = "pending"  # pending | approved | rejected
    note: str = ""
    decided_by: str = ""
    created_at: float = field(default_factory=time.time)
    decided_at: float | None = None


@dataclass
class Conversation:
    id: str
    customer: str
    mode: str = "ai"  # ai | queued | active
    handoff_reason: str = ""
    agent_name: str = ""
    messages: list[Message] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def summary(self) -> dict:
        last = next((m for m in reversed(self.messages) if m.role != "approval"), None)
        return {
            "id": self.id,
            "customer": self.customer,
            "mode": self.mode,
            "handoff_reason": self.handoff_reason,
            "agent_name": self.agent_name,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_message": asdict(last) if last else None,
        }


conversations: dict[str, Conversation] = {}
approvals: dict[str, Approval] = {}


# ----------------------------------------------------------- live updates (SSE)

class Hub:
    def __init__(self) -> None:
        self.subs: dict[str, set[asyncio.Queue]] = {}

    def publish(self, topic: str, payload: dict) -> None:
        for q in self.subs.get(topic, ()):
            q.put_nowait(payload)

    async def stream(self, topic: str):
        q: asyncio.Queue = asyncio.Queue()
        self.subs.setdefault(topic, set()).add(q)
        try:
            yield "retry: 2000\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            self.subs[topic].discard(q)


hub = Hub()


def sse(topic: str) -> StreamingResponse:
    return StreamingResponse(
        hub.stream(topic),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def notify(conv: Conversation, payload: dict) -> None:
    """Send an update to the customer of `conv` and to every console."""
    hub.publish(f"session:{conv.id}", payload)
    hub.publish("console", {**payload, "session_id": conv.id})


def add_message(conv: Conversation, msg: Message) -> Message:
    conv.messages.append(msg)
    conv.updated_at = msg.ts
    notify(conv, {"type": "message", "message": asdict(msg)})
    hub.publish("console", {"type": "conversation", "conversation": conv.summary()})
    return msg


def set_mode(conv: Conversation, mode: str, **fields: str) -> None:
    conv.mode = mode
    for k, v in fields.items():
        setattr(conv, k, v)
    notify(conv, {"type": "mode", "mode": mode, "agent_name": conv.agent_name})
    hub.publish("console", {"type": "conversation", "conversation": conv.summary()})


def publish_approval(conv: Conversation, ap: Approval) -> None:
    notify(conv, {"type": "approval", "approval": asdict(ap)})


def typing(conv: Conversation, who: str, on: bool) -> None:
    notify(conv, {"type": "typing", "who": who, "on": on})


# ------------------------------------------------------------- agent runs

def handle_event(conv: Conversation, event: Event) -> None:
    """Turn one ADK event into transcript messages and human work items."""
    if not event.content or not event.content.parts or event.partial:
        return
    lr_ids = event.long_running_tool_ids or set()
    for part in event.content.parts:
        fc = part.function_call
        if fc and fc.name == CONFIRMATION_CALL:
            original = (fc.args or {}).get("originalFunctionCall", {})
            create_approval(conv, "confirmation", original.get("name", ""), original.get("args", {}), fc.id)
        elif fc and fc.id in lr_ids:
            create_approval(conv, "long_running", fc.name, dict(fc.args or {}), fc.id)
        elif fc and fc.name == "transfer_to_human":
            reason = (fc.args or {}).get("reason", "")
            set_mode(conv, "queued", handoff_reason=reason)
            add_message(conv, Message("system", "Connecting you with the Shopio team…"))
        elif part.text and part.text.strip() and event.author == root_agent.name:
            add_message(conv, Message("ai", part.text.strip()))


def create_approval(conv: Conversation, kind: str, tool: str, args: dict, call_id: str) -> None:
    ap = Approval(
        id=uuid.uuid4().hex[:6].upper(), session_id=conv.id, customer=conv.customer,
        kind=kind, tool=tool, args=args, call_id=call_id,
    )
    approvals[ap.id] = ap
    publish_approval(conv, ap)
    add_message(conv, Message("approval", approval_id=ap.id))


async def run_agent(conv: Conversation, content: types.Content) -> None:
    async with conv.lock:
        show_typing = conv.mode == "ai"
        if show_typing:
            typing(conv, "ai", True)
        try:
            async for event in runner.run_async(
                user_id=USER_ID, session_id=conv.id, new_message=content
            ):
                handle_event(conv, event)
        except Exception as exc:  # keep the chat usable if the model call fails
            add_message(conv, Message("system", f"The assistant hit an error: {exc}"))
        finally:
            if show_typing:
                typing(conv, "ai", False)


async def record_human_event(conv: Conversation, *, text: str = "", state: dict | None = None) -> None:
    """Write a human agent's message or a state change into the ADK session, so
    the agent sees it when it takes the conversation back."""
    session = await session_service.get_session(app_name=APP_NAME, user_id=USER_ID, session_id=conv.id)
    event = Event(
        invocation_id="human-" + uuid.uuid4().hex[:8],
        author="human_agent",
        content=types.Content(role="model", parts=[types.Part(text=text)]) if text else None,
        actions=EventActions(state_delta=state or {}),
    )
    await session_service.append_event(session, event)


def get_conv(session_id: str) -> Conversation:
    conv = conversations.get(session_id)
    if not conv:
        raise HTTPException(404, "Unknown conversation")
    return conv


# -------------------------------------------------------------- customer API

class NewSession(BaseModel):
    name: str = ""


class Text(BaseModel):
    text: str


@app.post("/api/sessions")
async def create_session(body: NewSession) -> dict:
    session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID)
    conv = Conversation(id=session.id, customer=body.name.strip() or "Guest")
    conversations[conv.id] = conv
    hello = f"Hi {conv.customer}! " if body.name.strip() else "Hi! "
    add_message(conv, Message("ai", hello + "I'm Shopio AI. I can help with refunds and budget "
                              "requests, or connect you to the Shopio team. What can I do for you?"))
    return {"session_id": conv.id}


@app.get("/api/sessions/{session_id}")
def session_snapshot(session_id: str) -> dict:
    conv = get_conv(session_id)
    return {
        **conv.summary(),
        "messages": [asdict(m) for m in conv.messages],
        "approvals": {a.id: asdict(a) for a in approvals.values() if a.session_id == conv.id},
    }


@app.get("/api/sessions/{session_id}/stream")
def session_stream(session_id: str) -> StreamingResponse:
    get_conv(session_id)
    return sse(f"session:{session_id}")


@app.post("/api/sessions/{session_id}/messages")
async def customer_message(session_id: str, body: Text) -> dict:
    conv = get_conv(session_id)
    text = body.text.strip()
    if not text:
        raise HTTPException(400, "Empty message")
    add_message(conv, Message("customer", text, author=conv.customer))
    # Always go through the runner: during a handoff the before_model_callback
    # skips the model but the message is still recorded in the ADK session.
    asyncio.create_task(run_agent(conv, types.Content(role="user", parts=[types.Part(text=text)])))
    return {"ok": True}


# --------------------------------------------------------------- console API

class Decision(BaseModel):
    approved: bool
    note: str = ""
    agent_name: str = "Supervisor"


class AgentAction(BaseModel):
    agent_name: str = "Support agent"
    text: str = ""


@app.get("/api/console/snapshot")
def console_snapshot() -> dict:
    return {
        "conversations": [c.summary() for c in conversations.values()],
        "approvals": [asdict(a) for a in approvals.values()],
    }


@app.get("/api/console/stream")
def console_stream() -> StreamingResponse:
    return sse("console")


@app.get("/api/console/conversations/{session_id}")
def console_conversation(session_id: str) -> dict:
    return session_snapshot(session_id)


@app.post("/api/console/approvals/{approval_id}")
async def decide(approval_id: str, body: Decision) -> dict:
    ap = approvals.get(approval_id)
    if not ap:
        raise HTTPException(404, "Unknown approval")
    if ap.status != "pending":
        raise HTTPException(409, f"Already {ap.status}")
    conv = get_conv(ap.session_id)
    ap.status = "approved" if body.approved else "rejected"
    ap.note, ap.decided_by, ap.decided_at = body.note.strip(), body.agent_name.strip(), time.time()
    publish_approval(conv, ap)

    if ap.kind == "confirmation":
        response = types.FunctionResponse(
            id=ap.call_id, name=CONFIRMATION_CALL,
            response={"confirmed": body.approved, "payload": {"note": ap.note}},
        )
    else:
        response = types.FunctionResponse(
            id=ap.call_id, name=ap.tool,
            response={"status": ap.status, "note": ap.note, "decided_by": ap.decided_by},
        )
    asyncio.create_task(run_agent(conv, types.Content(role="user", parts=[types.Part(function_response=response)])))
    return asdict(ap)


@app.post("/api/console/conversations/{session_id}/join")
async def join(session_id: str, body: AgentAction) -> dict:
    conv = get_conv(session_id)
    if conv.mode == "active":
        raise HTTPException(409, f"{conv.agent_name} is already in this chat")
    name = body.agent_name.strip() or "Support agent"
    async with conv.lock:
        await record_human_event(conv, state={HANDOFF_STATUS: "active"})
    set_mode(conv, "active", agent_name=name)
    add_message(conv, Message("system", f"{name} joined the chat"))
    return conv.summary()


@app.post("/api/console/conversations/{session_id}/messages")
async def human_message(session_id: str, body: AgentAction) -> dict:
    conv = get_conv(session_id)
    if conv.mode != "active":
        raise HTTPException(409, "Join the chat first")
    text = body.text.strip()
    if not text:
        raise HTTPException(400, "Empty message")
    async with conv.lock:
        await record_human_event(conv, text=text)
    add_message(conv, Message("human", text, author=conv.agent_name))
    return {"ok": True}


@app.post("/api/console/conversations/{session_id}/release")
async def release(session_id: str) -> dict:
    conv = get_conv(session_id)
    name = conv.agent_name or "The team"
    async with conv.lock:
        await record_human_event(conv, state={HANDOFF_STATUS: None, HANDOFF_REASON: None})
    set_mode(conv, "ai", agent_name="", handoff_reason="")
    add_message(conv, Message("system", f"{name} handed the chat back to Shopio AI"))
    return conv.summary()


@app.post("/api/console/conversations/{session_id}/typing")
def human_typing(session_id: str) -> dict:
    conv = get_conv(session_id)
    if conv.mode == "active":
        hub.publish(f"session:{conv.id}", {"type": "typing", "who": "human", "on": True})
    return {"ok": True}


# -------------------------------------------------------------------- pages

@app.get("/")
def customer_page() -> FileResponse:
    return FileResponse(STATIC / "customer.html")


@app.get("/console")
def console_page() -> FileResponse:
    return FileResponse(STATIC / "console.html")
