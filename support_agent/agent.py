"""Customer support agent with two kinds of human-in-the-loop.

Tool-level (the agent asks a supervisor to sign off on one action; the
customer keeps talking to the agent):
  * request_refund          -> LongRunningFunctionTool
                               Refunds up to REFUND_AUTO_LIMIT are paid at once.
                               Larger ones return "pending"; the chat service
                               pays them when a supervisor approves and sends the
                               result back as the final FunctionResponse for the
                               same call id.

Conversation-level (the customer wants a person; the agent steps aside):
  * transfer_to_human       -> escalate tool, sets session state
  * human_takeover_guard    -> before_model_callback; while a human owns the
                               chat the model is never called.

The chat service (chat_service.py) watches the events these produce and routes
them to the supervisor / human-agent console (console_service.py).
"""

import os
import uuid

from google.adk.agents import LlmAgent
from google.adk.agents.callback_context import CallbackContext
from google.adk.models import LlmRequest, LlmResponse
from google.adk.tools import LongRunningFunctionTool, ToolContext
from google.genai import types

from . import orders

MODEL = os.getenv("SUPPORT_AGENT_MODEL", "gemini-3.8-flash")
REFUND_AUTO_LIMIT = float(os.getenv("REFUND_AUTO_LIMIT", "120"))

# Session state keys
HANDOFF_STATUS = "handoff_status"  # None | "queued" | "active"
HANDOFF_REASON = "handoff_reason"
CUSTOMER_NAME = "customer_name"  # set from the pre-chat screen


# ------------------------------------------------------------------- orders

def search_orders(query: str = "") -> dict:
    """Look up the customer's recent orders in the order database.

    Call this before every refund so the customer can pick the order. The app
    shows the results as cards the customer can tap.

    Args:
        query: Optional order id or product name. Leave empty to list recent orders.
    """
    hits = orders.search(query)
    matched = bool(hits) or not query
    if not hits:  # nothing matched: show recent orders rather than nothing
        hits = orders.search("")
    for o in hits:
        o["refund_pending"] = o["order_id"] in PENDING_REFUNDS
        o["refundable"] = o["refundable"] and not o["refund_pending"]
    return {"matched": matched, "orders": hits}


# ---------------------------------------------------------------- tool-level

# Refunds waiting for a supervisor: order_id -> function call id. In memory, like
# the rest of the demo's state.
PENDING_REFUNDS: dict[str, str] = {}


def refund_problem(order_id: str, amount: float) -> str | None:
    order = orders.get(order_id)
    if not order:
        return f"Order {order_id} was not found. Call search_orders and let the customer pick one."
    if order["refunded"]:
        return f"Order {order_id} was already refunded ({order['refund_id']})."
    if order_id in PENDING_REFUNDS:
        return f"A refund for order {order_id} is already waiting for a supervisor."
    if order["status"] != "delivered":
        return f"Order {order_id} is {order['status']}, so it cannot be refunded."
    if amount <= 0 or amount > order["total"]:
        return f"Amount must be between 1 and the order total ({order['total']:g})."
    return None


def pay_refund(order_id: str, amount: float) -> dict:
    """Record the refund. Called by the tool for small refunds and by the chat
    service when a supervisor approves a large one."""
    refund_id = "RF-" + uuid.uuid4().hex[:6].upper()
    orders.mark_refunded(order_id, refund_id, amount)
    return {"status": "refunded", "refund_id": refund_id, "order_id": order_id, "amount": amount}


def request_refund(order_id: str, amount: float, reason: str, tool_context: ToolContext) -> dict:
    """Refund an order the customer picked from search_orders.

    Refunds up to the auto-approval limit are paid at once (status "refunded").
    Larger ones are sent to a supervisor and return status "pending"; tell the
    customer it is being reviewed and that they can keep chatting. The
    supervisor's decision arrives later as the final result of this call.

    Args:
        order_id: The order to refund, from search_orders.
        amount: Refund amount; the order total unless the customer asks for less.
        reason: Why the customer is being refunded.
    """
    order_id = order_id.strip().upper()
    if problem := refund_problem(order_id, amount):
        return {"status": "error", "message": problem}
    if amount <= REFUND_AUTO_LIMIT:
        return pay_refund(order_id, amount)
    PENDING_REFUNDS[order_id] = tool_context.function_call_id
    return {
        "status": "pending",
        "ticket_id": tool_context.function_call_id,
        "order_id": order_id,
        "amount": amount,
        "message": f"Refunds over {REFUND_AUTO_LIMIT:g} need a supervisor. Sent for review.",
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
    instruction=f"""You are Shopio AI, the friendly customer support assistant for Shopio,
an online store.
Reply in the customer's language. Keep replies short.
You are talking to {{{CUSTOMER_NAME}?}} (the name they gave before the chat). Use their
name now and then, for example when greeting or confirming something important,
but not in every message.

- Orders: when the customer wants to see their orders or check an order's status,
  call search_orders. The app shows them as cards, so do not list them again;
  reply in one short sentence and offer help (for example a refund). If they then
  pick an order, tell them its status and ask what they need.
- Refunds, in this order:
  1. When the customer asks for a refund, your FIRST step is always a
     search_orders call, before you write any text (pass an order id or product if
     they gave one). The customer only sees their orders if you call it, so never
     ask them to pick an order without calling it in the same turn.
     Never guess an order id. The app shows the orders as cards, so do not list
     them again; just ask the customer, in one short sentence, to pick the order.
  2. When they pick one, ask for the reason if they have not said it.
  3. Call request_refund with the full order total, unless they ask for a smaller amount.
  Only delivered orders can be refunded. If request_refund returns "refunded", give
  the refund id. If it returns "pending" (over {REFUND_AUTO_LIMIT:,.0f}), say a
  supervisor is reviewing it and they can keep chatting meanwhile; do not call it
  again for that order. When the decision arrives later, tell them the outcome
  (with the refund id if approved) and any note from the supervisor, politely if
  it was rejected. If request_refund returns an error, explain it simply.
- General questions: answer from the store policy below. If a detail is not
  covered, say so briefly and offer to connect them with the team; do not transfer
  unless they agree.
- If the customer asks for a human or is clearly frustrated, call
  transfer_to_human with a short summary. Do not say anything after calling it.

Store policy:
- Returns: within 30 days of delivery, item unused with tags. Damaged or wrong
  items can be returned any time within 30 days at no cost.
- Refunds go back to the original payment method within 5-7 business days.
- Shipping: free over 500 baht, otherwise 40 baht; 2-4 business days in Thailand.
- Support hours for the human team: 9:00-18:00, Monday-Saturday.
- Messages from a human support agent may appear earlier in the conversation;
  continue from where they left off.""",
    tools=[
        search_orders,
        LongRunningFunctionTool(request_refund),
        transfer_to_human,
    ],
    before_model_callback=human_takeover_guard,
)
