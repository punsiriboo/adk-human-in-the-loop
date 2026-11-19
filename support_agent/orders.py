"""SQLite order store for the demo.

data/orders.db is created from data/orders.sql the first time it is needed.
Delete the .db file to reset it.
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
DB_PATH = Path(os.getenv("ORDERS_DB", DATA / "orders.db"))
SEED_PATH = DATA / "orders.sql"

_SELECT = """
    SELECT o.order_id, o.order_date, o.status, o.total, o.refund_id, o.refunded_amount,
           group_concat(i.name || CASE WHEN i.qty > 1 THEN ' x' || i.qty ELSE '' END, ', ') AS items
    FROM orders o LEFT JOIN order_items i ON i.order_id = o.order_id
"""


def _connect() -> sqlite3.Connection:
    fresh = not DB_PATH.exists()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if fresh:
        conn.executescript(SEED_PATH.read_text(encoding="utf-8"))
        conn.commit()
    return conn


def _row(r: sqlite3.Row) -> dict:
    d = dict(r)
    d["refunded"] = d["refund_id"] is not None
    d["refundable"] = d["status"] == "delivered" and not d["refunded"]
    return d


def search(query: str = "", limit: int = 10) -> list[dict]:
    """Recent orders, newest first, matching an order id or product name."""
    q = f"%{query.strip()}%"
    with _connect() as conn:
        rows = conn.execute(
            _SELECT + """
            WHERE ? = '%%' OR o.order_id LIKE ? OR o.order_id IN
                  (SELECT order_id FROM order_items WHERE name LIKE ?)
            GROUP BY o.order_id ORDER BY o.order_date DESC LIMIT ?""",
            (q, q, q, limit),
        ).fetchall()
    return [_row(r) for r in rows]


def get(order_id: str) -> dict | None:
    with _connect() as conn:
        r = conn.execute(_SELECT + " WHERE o.order_id = ? GROUP BY o.order_id", (order_id.strip().upper(),)).fetchone()
    return _row(r) if r and r["order_id"] else None


def mark_refunded(order_id: str, refund_id: str, amount: float) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE orders SET refund_id = ?, refunded_amount = ?, refunded_at = ? WHERE order_id = ?",
            (refund_id, amount, datetime.now().isoformat(timespec="seconds"), order_id),
        )
