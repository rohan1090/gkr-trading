"""PendingOrderRegistry — mutable recovery surface for order tracking.

SQLite-backed registry tracking all non-terminal orders.
UNKNOWN status for timeout/crash recovery.
Restart must reconcile UNKNOWN before any resubmission.
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import List, Optional

from gkr_trading.core.order_model import OrderStatus

_DDL = """\
CREATE TABLE IF NOT EXISTS pending_orders (
    client_order_id TEXT PRIMARY KEY,
    intent_id       TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    instrument_ref_json TEXT NOT NULL,
    action          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending_local',
    venue_order_id  TEXT,
    quantity        INTEGER NOT NULL,
    limit_price_cents INTEGER,
    created_at_ns   INTEGER NOT NULL,
    updated_at_ns   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_po_session ON pending_orders(session_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON pending_orders(status);
"""

_TERMINAL = frozenset({
    OrderStatus.FILLED.value,
    OrderStatus.CANCELED.value,
    OrderStatus.REJECTED.value,
    OrderStatus.EXPIRED.value,
})


class PendingOrderRegistry:
    """SQLite-backed order registry for crash recovery and duplicate prevention."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._conn.executescript(_DDL)

    def register(
        self,
        *,
        client_order_id: str,
        intent_id: str,
        session_id: str,
        instrument_ref_json: str,
        action: str,
        venue: str,
        quantity: int,
        limit_price_cents: Optional[int] = None,
    ) -> bool:
        """Register a new pending order. Returns False if duplicate (idempotent)."""
        now_ns = time.time_ns()
        try:
            self._conn.execute(
                """INSERT INTO pending_orders
                   (client_order_id, intent_id, session_id, instrument_ref_json,
                    action, venue, status, quantity, limit_price_cents,
                    created_at_ns, updated_at_ns)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (client_order_id, intent_id, session_id, instrument_ref_json,
                 action, venue, OrderStatus.PENDING_LOCAL.value, quantity,
                 limit_price_cents, now_ns, now_ns),
            )
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate client_order_id — idempotent
            return False

    def update_status(
        self,
        client_order_id: str,
        status: OrderStatus,
        venue_order_id: Optional[str] = None,
    ) -> None:
        """Update order status. No-op if order doesn't exist."""
        now_ns = time.time_ns()
        params: list = [status.value, now_ns]
        set_clause = "status = ?, updated_at_ns = ?"
        if venue_order_id is not None:
            set_clause += ", venue_order_id = ?"
            params.append(venue_order_id)
        params.append(client_order_id)
        self._conn.execute(
            f"UPDATE pending_orders SET {set_clause} WHERE client_order_id = ?",
            params,
        )
        self._conn.commit()

    def mark_all_non_terminal_as_unknown(self, session_id: str) -> int:
        """On startup: mark all non-terminal orders as UNKNOWN for reconciliation.

        Returns count of orders marked.
        """
        now_ns = time.time_ns()
        terminal_str = ", ".join(f"'{s}'" for s in _TERMINAL)
        cur = self._conn.execute(
            f"""UPDATE pending_orders
                SET status = ?, updated_at_ns = ?
                WHERE session_id = ? AND status NOT IN ({terminal_str})""",
            (OrderStatus.UNKNOWN.value, now_ns, session_id),
        )
        self._conn.commit()
        return cur.rowcount

    def get_unknown_orders(self, session_id: str) -> List[dict]:
        """Get all UNKNOWN orders for reconciliation."""
        cur = self._conn.execute(
            """SELECT client_order_id, intent_id, instrument_ref_json, action,
                      venue, venue_order_id, quantity, limit_price_cents,
                      created_at_ns
               FROM pending_orders
               WHERE session_id = ? AND status = ?""",
            (session_id, OrderStatus.UNKNOWN.value),
        )
        rows = cur.fetchall()
        return [
            {
                "client_order_id": r[0],
                "intent_id": r[1],
                "instrument_ref_json": r[2],
                "action": r[3],
                "venue": r[4],
                "venue_order_id": r[5],
                "quantity": r[6],
                "limit_price_cents": r[7],
                "created_at_ns": r[8],
            }
            for r in rows
        ]

    def get_active_orders(self, session_id: str) -> List[dict]:
        """Get all non-terminal orders for a session."""
        terminal_str = ", ".join(f"'{s}'" for s in _TERMINAL)
        cur = self._conn.execute(
            f"""SELECT client_order_id, intent_id, status, instrument_ref_json,
                       action, venue, venue_order_id, quantity
                FROM pending_orders
                WHERE session_id = ? AND status NOT IN ({terminal_str})""",
            (session_id,),
        )
        rows = cur.fetchall()
        return [
            {
                "client_order_id": r[0],
                "intent_id": r[1],
                "status": r[2],
                "instrument_ref_json": r[3],
                "action": r[4],
                "venue": r[5],
                "venue_order_id": r[6],
                "quantity": r[7],
            }
            for r in rows
        ]

    def exists(self, client_order_id: str) -> bool:
        """Check if a client_order_id already exists (duplicate prevention)."""
        cur = self._conn.execute(
            "SELECT 1 FROM pending_orders WHERE client_order_id = ?",
            (client_order_id,),
        )
        return cur.fetchone() is not None

    def get_status(self, client_order_id: str) -> Optional[str]:
        """Get current status of an order."""
        cur = self._conn.execute(
            "SELECT status FROM pending_orders WHERE client_order_id = ?",
            (client_order_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
