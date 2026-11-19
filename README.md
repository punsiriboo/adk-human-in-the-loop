# Shopio AI: human-in-the-loop support demo

Shopio AI, a customer support agent built with Google ADK, that hands work to people in two ways:

| | Tool-level | Conversation-level |
|---|---|---|
| Who wants the human | The **agent**: one action needs sign-off | The **customer**: they want a person |
| Flow | Agent → Supervisor → Agent | Agent → Human agent → Agent (optional) |
| ADK mechanism | `LongRunningFunctionTool` | escalate tool, `before_model_callback`, session state |
| In this demo | Refund over **120** (`request_refund`) | `transfer_to_human` + `human_takeover_guard` |

## Two services

| Service | File | Port | Serves | Who uses it |
|---|---|---|---|---|
| **Chat service** | `chat_service.py` | 8000 | Customer chat + the ADK agent. Owns every conversation and approval. | Customers (public) |
| **Console service** | `console_service.py` | 8100 | Support console. Holds no state; relays to the chat service. | Supervisors and human agents (staff only) |

```
customer browser ──HTTP + SSE──▶ chat service (ADK) ◀──/internal: SSE events, HTTP calls── console service ◀──HTTP + SSE── staff browser
```

- **Customer chat** at `http://localhost:8000/`: a pre-chat screen asks for the customer's name (stored in ADK session state as `customer_name`, so the agent knows who it is talking to), then chat with Shopio AI, order cards to pick from, live approval status cards, a queue banner and human-agent takeover.
- **Support console** at `http://localhost:8100/`: an approvals inbox (approve or decline with a note) and a conversations inbox (accept a queued chat, take over from the AI, reply, leave the chat).

The console service subscribes to the chat service's `/internal/stream` (SSE) and sends decisions, replies and handoffs to `/internal/*` over HTTP. Set the same `INTERNAL_TOKEN` on both services to require a shared secret on those calls.

## Run

```bash
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example support_agent/.env      # add GOOGLE_API_KEY (or GEMINI_API_KEY)

uvicorn chat_service:app --port 8000      # terminal 1
uvicorn console_service:app --port 8100   # terminal 2
```

Open the customer chat and the console side by side, then try the chips:

- **My orders**: the customer's orders appear as cards.
- **Refund**: pick an order, give a reason. Orders at or under 120 are refunded straight away. Larger ones go to the console as pending; the customer keeps chatting, and the refund is paid only when a supervisor approves.
- **Talk with a human**: the chat moves to the console queue and the AI stops replying until the agent leaves the chat.

## How it fits together

The architecture and step-by-step sequence diagrams for each flow are in [docs/document.html](docs/document.html) (open it in a browser).

- `support_agent/agent.py`: the ADK agent and its tools. `REFUND_AUTO_LIMIT` (default 120) sets when a refund needs approval.
- `support_agent/orders.py` + `data/orders.sql`: demo order store (SQLite). `data/orders.db` is built from the SQL on first run; delete it to reset refunds.
- `chat_service.py`: runs the agent and watches its events:
  - `search_orders` result → order cards in the customer chat.
  - `request_refund` (a `LongRunningFunctionTool`) returns `pending` for refunds over the limit → approval item. On approve, the chat service pays the refund (`pay_refund`); either way the outcome goes back to the agent as the final `FunctionResponse` for the same call id.
  - `transfer_to_human` call → chat queued for a human. Joining sets `handoff_status=active` in session state; the human's messages are written to the ADK session, so the AI has the full history when the chat is handed back.
- `console_service.py`: serves the console and relays `/api/console/*` to the chat service's `/internal/*`.
- `static/customer.html`, `static/console.html`: the two UIs (no build step).
- `static/brand/`: Shopio logo, bag mark, mascot poses and avatar, resized from `../shopio/images`. Colours and fonts follow the `:root` tokens in the Shopio deck.

## Deploying to Cloud Run

Deploy the two services separately. Before running more than one instance of the chat service, move its state out of memory:

- ADK sessions: `DatabaseSessionService` (Cloud SQL) or `VertexAiSessionService` instead of `InMemorySessionService`.
- Conversations and approvals: Firestore or Cloud SQL instead of the in-process dicts.
- Live events across instances: Redis or Pub/Sub behind the SSE hub.
- Orders: a real order API or Cloud SQL; the SQLite file does not survive restarts.

Until then, run the chat service with `--max-instances=1`. Put the console service behind IAP, and allow only the console service to call the chat service's `/internal/*` (plus `INTERNAL_TOKEN`).

State is kept in memory, so a restart of the chat service clears all chats.
