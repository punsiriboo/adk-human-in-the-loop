# ADK human-in-the-loop support demo

A customer support agent built with Google ADK that hands work to people in two ways:

| | Tool-level | Conversation-level |
|---|---|---|
| Who wants the human | The **agent**: one action needs sign-off | The **customer**: they want a person |
| Flow | Agent → Supervisor → Agent | Agent → Human agent → Agent (optional) |
| ADK mechanism | `require_confirmation`, `LongRunningFunctionTool` | escalate tool, `before_model_callback`, session state |
| In this demo | Refund over **120** (`issue_refund`), budget requests (`request_budget_approval`) | `transfer_to_human` + `human_takeover_guard` |

## Screens

- **Customer chat** at `http://localhost:8000/`: chat with Aria (the AI assistant), live approval status cards, a queue banner and human-agent takeover.
- **Support console** at `http://localhost:8000/console`: an approvals inbox (approve or decline with a note) and a conversations inbox (accept a queued chat, take over from the AI, reply, hand back to the AI).

Both screens update live over Server-Sent Events.

## Run

```bash
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example support_agent/.env      # add GOOGLE_API_KEY

uvicorn server:app --port 8000
```

Open the customer chat and the console side by side, then try:

- `I'd like a refund of 80 for order A1001`: refunded straight away (at or under the limit).
- `ขอคืนเงิน 450 บาท ออเดอร์ A1002`: paused; approve it in the console and the refund runs.
- `ขออนุมัติงบ 20,000 สำหรับจัดงานทีม`: pending; keep chatting, then decide in the console.
- `I'd like to talk to a person`: the chat moves to the console queue and the AI stops replying.

## How it fits together

- `support_agent/agent.py`: the ADK agent and its tools. `REFUND_AUTO_LIMIT` (default 120) sets when a refund needs approval.
- `server.py`: FastAPI. Runs the agent and watches its events:
  - `adk_request_confirmation` call → approval item; the decision goes back as a `FunctionResponse` with `{"confirmed": bool}`.
  - long-running tool call → approval item; the decision goes back as the final `FunctionResponse` for the same call id.
  - `transfer_to_human` call → chat queued for a human. Joining sets `handoff_status=active` in session state; the human's messages are written to the ADK session, so the AI has the full history when the chat is handed back.
- `static/customer.html`, `static/console.html`: the two UIs (no build step).

State is kept in memory, so a restart clears all chats.
