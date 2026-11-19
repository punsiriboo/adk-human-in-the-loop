"""Customer support agent with two kinds of human-in-the-loop.

Tool-level (the agent asks a supervisor to sign off on one action; the
customer keeps talking to the agent):
  * issue_refund            -> FunctionTool(require_confirmation=...)
                               Refunds above REFUND_AUTO_LIMIT pause until a
                               supervisor confirms or rejects the call.
  * request_budget_approval -> LongRunningFunctionTool
                               Returns "pending" at once; the supervisor's
                               decision is sent back later as the final
                               FunctionResponse for the same call id.

Conversation-level (the customer wants a person; the agent steps aside):
  * transfer_to_human       -> escalate tool, sets session state
  * human_takeover_guard    -> before_model_callback; while a human owns the
                               chat the model is never called.

The server (server.py) watches the events these produce and routes them to
the supervisor / human-agent console.
"""

import os
import uuid

from google.adk.agents import LlmAgent
from google.adk.agents.callback_context import CallbackContext
from google.adk.models import LlmRequest, LlmResponse
from google.adk.tools import FunctionTool, LongRunningFunctionTool, ToolContext
from google.genai import types

MODEL = os.getenv("SUPPORT_AGENT_MODEL", "gemini-2.5-flash")
REFUND_AUTO_LIMIT = float(os.getenv("REFUND_AUTO_LIMIT", "120"))

# Session state keys
HANDOFF_STATUS = "handoff_status"  # None | "queued" | "active"
HANDOFF_REASON = "handoff_reason"


# ---------------------------------------------------------------- tool-level

def issue_refund(order_id: str, amount: float, reason: str) -> dict:
    """Refund a customer order.

    Refunds above the auto-approval limit are held for a supervisor
    automatically; you do not need to ask for approval yourself.

    Args:
        order_id: The order to refund.
        amount: Refund amount.
        reason: Why the customer is being refunded.
    """
    return {
        "status": "refunded",
        "refund_id": "RF-" + uuid.uuid4().hex[:6].upper(),
        "order_id": order_id,
        "amount": amount,
    }


def _refund_needs_supervisor(order_id: str, amount: float, reason: str) -> bool:
    return amount > REFUND_AUTO_LIMIT


def request_budget_approval(amount: float, reason: str, tool_context: ToolContext) -> dict:
    """Submit a spending request to a human supervisor.

    The decision is not immediate. This returns "pending"; the supervisor's
    decision arrives later in a follow-up response.

    Args:
        amount: The amount requested.
        reason: What the money is for.
    """
    return {
        "status": "pending",
        "ticket_id": tool_context.function_call_id,
        "message": "Sent to a supervisor for review.",
    }


# -------------------------------------------------------- conversation-level

def transfer_to_human(reason: str, tool_context: ToolContext) -> dict:
    """Hand the conversation to a human support agent.

    Use when the customer asks for a person, is upset, or needs something you
    cannot do.

    Args:
        reason: One-line summary for the human agent of what the customer needs.
    """
    tool_context.state[HANDOFF_STATUS] = "queued"
    tool_context.state[HANDOFF_REASON] = reason
    return {"status": "queued", "message": "Customer is waiting for a human agent."}


def human_takeover_guard(
    callback_context: CallbackContext, llm_request: LlmRequest
) -> LlmResponse | None:
    """Keep the model quiet while the conversation belongs to a human.

    The customer's messages are still recorded in the session, so when the
    human hands the chat back the agent has the full history.
    """
    if callback_context.state.get(HANDOFF_STATUS) in ("queued", "active"):
        return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="")]))
    return None


root_agent = LlmAgent(
    model=MODEL,
    name="support_agent",
    instruction=f"""You are a friendly customer support agent for an online store.
Reply in the customer's language. Keep replies short.

- Refunds: use issue_refund. Refunds over {REFUND_AUTO_LIMIT:,.0f} are checked by a
  supervisor before they go through; if one is rejected, tell the customer politely.
- Spending or budget requests: use request_budget_approval. Tell the customer it is
  with a supervisor and they can keep chatting meanwhile. When the decision arrives,
  tell them the outcome and any note from the supervisor.
- If the customer asks for a human, is clearly frustrated, or you cannot help,
  call transfer_to_human with a short summary. Do not say anything after calling it.
- Messages from a human support agent may appear earlier in the conversation;
  continue from where they left off.""",
    tools=[
        FunctionTool(issue_refund, require_confirmation=_refund_needs_supervisor),
        LongRunningFunctionTool(request_budget_approval),
        transfer_to_human,
    ],
    before_model_callback=human_takeover_guard,
)
