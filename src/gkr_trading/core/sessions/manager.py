from __future__ import annotations

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.persistence.event_store import EventStore


class SessionManager:
    """Coordinates session identity; persistence is via EventStore."""

    def __init__(self, store: EventStore, session_id: SessionId) -> None:
        self._store = store
        self._session_id = session_id

    @property
    def session_id(self) -> SessionId:
        return self._session_id

    def append(self, event: CanonicalEvent) -> int:
        return self._store.append(str(self._session_id), event)
