"""Session-scoped broker reconciliation JSON (cursor, emitted ids, tracked orders)."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any


DDL_BROKER_RECONCILIATION = """
CREATE TABLE IF NOT EXISTS broker_reconciliation_state (
    session_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);
"""


def ensure_broker_reconciliation_table(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL_BROKER_RECONCILIATION)
    conn.commit()


def load_broker_reconciliation_payload(
    conn: sqlite3.Connection, session_id: str
) -> dict[str, Any] | None:
    ensure_broker_reconciliation_table(conn)
    row = conn.execute(
        "SELECT payload_json FROM broker_reconciliation_state WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    if not row:
        return None
    return json.loads(row[0])


def save_broker_reconciliation_payload(
    conn: sqlite3.Connection, session_id: str, payload: dict[str, Any]
) -> None:
    ensure_broker_reconciliation_table(conn)
    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT INTO broker_reconciliation_state(session_id, payload_json, updated_at_utc)
        VALUES (?,?,?)
        ON CONFLICT(session_id) DO UPDATE SET
          payload_json = excluded.payload_json,
          updated_at_utc = excluded.updated_at_utc
        """,
        (session_id, json.dumps(payload, sort_keys=True), ts),
    )
    conn.commit()
