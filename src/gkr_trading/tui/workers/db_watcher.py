"""Background DB event watcher — tails the event store for new events.

Runs in a Textual worker thread. Polls the SQLite event store every 3 s,
detects new events by comparing max(seq), and posts NewEventsMessage.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, List, Optional

from textual.message import Message

logger = logging.getLogger(__name__)


@dataclass
class EventSummary:
    """Lightweight event summary for display."""

    session_id: str
    seq: int
    event_type: str
    occurred_at: str
    payload_summary: str


class NewEventsMessage(Message):
    """Posted when new events are detected in the DB."""

    def __init__(self, events: list[EventSummary]) -> None:
        super().__init__()
        self.events = events


class SessionListMessage(Message):
    """Posted with refreshed session list data."""

    def __init__(self, sessions: list[dict]) -> None:
        super().__init__()
        self.sessions = sessions


class DBErrorMessage(Message):
    """Posted when DB access fails."""

    def __init__(self, error: str) -> None:
        super().__init__()
        self.error = error


class DBWatcher:
    """Stateful DB watcher — call ``poll_once`` repeatedly."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._last_seq: dict[str, int] = {}

    def _get_conn(self) -> Optional[sqlite3.Connection]:
        """Create a fresh connection for the calling thread."""
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as exc:
            logger.error(f"DB connect failed: {exc}")
            return None

    def list_sessions(self) -> list[dict]:
        """Return all distinct sessions with summary stats."""
        conn = self._get_conn()
        if not conn:
            return []
        try:
            cur = conn.execute(
                """SELECT session_id,
                          COUNT(*) as event_count,
                          MIN(envelope_json) as first_event,
                          MAX(seq) as max_seq
                   FROM events
                   GROUP BY session_id
                   ORDER BY MAX(seq) DESC"""
            )
            sessions = []
            for row in cur.fetchall():
                sid = row[0]
                event_count = row[1]
                max_seq = row[3]

                # Try to extract session metadata from events
                status = "unknown"
                strategy = ""
                stop_reason = ""
                try:
                    from gkr_trading.core.events import loads_event

                    # Check first event for session_started
                    first_cur = conn.execute(
                        "SELECT envelope_json FROM events WHERE session_id = ? ORDER BY seq ASC LIMIT 1",
                        (sid,),
                    )
                    first_row = first_cur.fetchone()
                    if first_row:
                        first_ev = loads_event(first_row[0])
                        if first_ev.event_type.value == "session_started":
                            payload = first_ev.payload
                            if hasattr(payload, "strategy_id"):
                                strategy = payload.strategy_id
                            elif isinstance(payload, dict):
                                strategy = payload.get("strategy_id", "")

                    # Check last event for session_stopped
                    last_cur = conn.execute(
                        "SELECT envelope_json FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1",
                        (sid,),
                    )
                    last_row = last_cur.fetchone()
                    if last_row:
                        last_ev = loads_event(last_row[0])
                        if last_ev.event_type.value == "session_stopped":
                            status = "stopped"
                            payload = last_ev.payload
                            if hasattr(payload, "stop_reason"):
                                stop_reason = payload.stop_reason
                            elif isinstance(payload, dict):
                                stop_reason = payload.get("stop_reason", "")
                        elif last_ev.event_type.value == "session_started":
                            status = "running"
                        else:
                            # Has events but no stop — could be running
                            status = "running"
                except Exception:
                    pass

                sessions.append({
                    "session_id": sid,
                    "event_count": event_count,
                    "status": status,
                    "strategy": strategy,
                    "stop_reason": stop_reason,
                    "max_seq": max_seq,
                })
            return sessions
        except Exception as exc:
            logger.error(f"list_sessions error: {exc}")
            return []
        finally:
            conn.close()

    def poll_events(self, session_id: str) -> list[EventSummary]:
        """Fetch new events for a session since last poll."""
        if not session_id:
            return []
        conn = self._get_conn()
        if not conn:
            return []

        try:
            last = self._last_seq.get(session_id, 0)
            cur = conn.execute(
                "SELECT session_id, seq, envelope_json FROM events "
                "WHERE session_id = ? AND seq > ? ORDER BY seq ASC",
                (session_id, last),
            )
            events = []
            for row in cur.fetchall():
                sid = row[0]
                seq = row[1]
                try:
                    from gkr_trading.core.events import loads_event

                    ev = loads_event(row[2])
                    payload_str = ""
                    if hasattr(ev.payload, "__dict__"):
                        d = {
                            k: v
                            for k, v in ev.payload.__dict__.items()
                            if v is not None and k != "self"
                        }
                        parts = []
                        for k, v in list(d.items())[:3]:
                            parts.append(f"{k}={v}")
                        payload_str = ", ".join(parts)
                    elif isinstance(ev.payload, dict):
                        parts = []
                        for k, v in list(ev.payload.items())[:3]:
                            parts.append(f"{k}={v}")
                        payload_str = ", ".join(parts)

                    events.append(
                        EventSummary(
                            session_id=sid,
                            seq=seq,
                            event_type=ev.event_type.value,
                            occurred_at=ev.occurred_at_utc,
                            payload_summary=payload_str[:80],
                        )
                    )
                    self._last_seq[sid] = max(self._last_seq.get(sid, 0), seq)
                except Exception as exc:
                    logger.debug(f"Event parse error seq={seq}: {exc}")

            return events
        except Exception as exc:
            logger.error(f"poll_events error: {exc}")
            return []
        finally:
            conn.close()

    def get_session_events(self, session_id: str) -> list[EventSummary]:
        """Load all events for a session (not just new ones)."""
        if not session_id:
            return []
        conn = self._get_conn()
        if not conn:
            return []
        try:
            cur = conn.execute(
                "SELECT session_id, seq, envelope_json FROM events "
                "WHERE session_id = ? ORDER BY seq ASC",
                (session_id,),
            )
            events = []
            for row in cur.fetchall():
                try:
                    from gkr_trading.core.events import loads_event

                    ev = loads_event(row[2])
                    payload_str = ""
                    if hasattr(ev.payload, "__dict__"):
                        d = {
                            k: v
                            for k, v in ev.payload.__dict__.items()
                            if v is not None
                        }
                        parts = [f"{k}={v}" for k, v in list(d.items())[:3]]
                        payload_str = ", ".join(parts)
                    elif isinstance(ev.payload, dict):
                        parts = [f"{k}={v}" for k, v in list(ev.payload.items())[:3]]
                        payload_str = ", ".join(parts)

                    events.append(
                        EventSummary(
                            session_id=row[0],
                            seq=row[1],
                            event_type=ev.event_type.value,
                            occurred_at=ev.occurred_at_utc,
                            payload_summary=payload_str[:80],
                        )
                    )
                except Exception:
                    pass
            return events
        except Exception as exc:
            logger.error(f"get_session_events error: {exc}")
            return []
        finally:
            conn.close()
