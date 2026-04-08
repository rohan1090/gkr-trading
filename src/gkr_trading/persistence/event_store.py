from __future__ import annotations

import sqlite3
from typing import Protocol

from gkr_trading.core.events import CanonicalEvent, dumps_event, loads_event

_DDL = """\
CREATE TABLE IF NOT EXISTS events (
    session_id   TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    envelope_json TEXT NOT NULL,
    PRIMARY KEY (session_id, seq)
);
"""


def enforce_wal_mode(conn: sqlite3.Connection) -> None:
    """Enforce WAL mode and synchronous=FULL for durability."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")


class EventStore(Protocol):
    def append(self, session_id: str, event: CanonicalEvent) -> int: ...

    def load_session(self, session_id: str) -> list[CanonicalEvent]: ...

    def max_seq(self, session_id: str) -> int: ...


class SqliteEventStore:
    def __init__(self, conn: sqlite3.Connection, *, init_schema: bool = True) -> None:
        self._conn = conn
        enforce_wal_mode(conn)
        if init_schema:
            conn.executescript(_DDL)

    def append(self, session_id: str, event: CanonicalEvent) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        nxt = int(row[0]) + 1
        self._conn.execute(
            "INSERT INTO events(session_id, seq, envelope_json) VALUES (?,?,?)",
            (session_id, nxt, dumps_event(event)),
        )
        self._conn.commit()
        return nxt

    def load_session(self, session_id: str) -> list[CanonicalEvent]:
        cur = self._conn.execute(
            "SELECT envelope_json FROM events WHERE session_id = ? ORDER BY seq ASC",
            (session_id,),
        )
        return [loads_event(r[0]) for r in cur.fetchall()]

    def max_seq(self, session_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return int(row[0])
