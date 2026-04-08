from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from gkr_trading.core.events import CanonicalEvent
from gkr_trading.core.events.types import EventType
from gkr_trading.core.portfolio import PortfolioState, apply_canonical_event
from gkr_trading.core.portfolio.anomalies import PortfolioAnomaly
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.persistence.event_store import EventStore

_SKIP_REPLAY: frozenset[EventType] = frozenset(
    {
        EventType.PORTFOLIO_UPDATED,
        EventType.SESSION_STARTED,
        EventType.SESSION_STOPPED,
        EventType.REPLAY_COMPLETED,
        EventType.SIGNAL_GENERATED,
        EventType.TRADE_INTENT_CREATED,
        EventType.RISK_APPROVED,
        # Control-plane events: audit-only, do not mutate portfolio
        EventType.OPERATOR_COMMAND,
        EventType.RECONCILIATION_COMPLETED,
        EventType.PENDING_ORDER_REGISTERED,
        EventType.ORDER_SUBMISSION_ATTEMPTED,
        # Options lifecycle events: these DO mutate positions but are
        # processed by the options-aware replay path, not by the
        # legacy equity-only apply_canonical_event. The legacy replay
        # skips them to avoid errors; the options-aware replay handles
        # them in replay_portfolio_state_v2.
        EventType.ASSIGNMENT_RECEIVED,
        EventType.EXERCISE_PROCESSED,
        EventType.EXPIRATION_PROCESSED,
        EventType.OPTIONS_ORDER_SUBMITTED,
    }
)


@dataclass(frozen=True)
class ReplayResult:
    """Replay output: portfolio state plus anomalies collected during permissive fold."""

    state: PortfolioState
    anomalies: tuple[PortfolioAnomaly, ...]


def replay_portfolio_state(
    events: list[CanonicalEvent],
    starting_cash: Decimal,
    *,
    strict: bool = False,
    anomalies: list[PortfolioAnomaly] | None = None,
) -> ReplayResult:
    """Reconstruct portfolio from execution + marks; skip audit-only snapshots.

    Anomalies are always collected into an internal buffer when ``anomalies`` is None
    (default-visible for operators). Pass an explicit list to append into a caller-owned buffer.
    """
    buf: list[PortfolioAnomaly] = [] if anomalies is None else anomalies
    s = PortfolioState.initial(starting_cash)
    for i, e in enumerate(events):
        if e.event_type in _SKIP_REPLAY:
            continue
        s = apply_canonical_event(s, e, strict=strict, anomalies=buf, event_index=i)
    return ReplayResult(state=s, anomalies=tuple(buf))


class ReplayEngine:
    def __init__(self, store: EventStore, starting_cash: Decimal) -> None:
        self._store = store
        self._starting_cash = starting_cash

    def replay_session(
        self,
        session_id: SessionId,
        *,
        strict: bool = False,
        anomalies: list[PortfolioAnomaly] | None = None,
    ) -> tuple[ReplayResult, list[CanonicalEvent]]:
        raw = self._store.load_session(str(session_id))
        result = replay_portfolio_state(
            raw, self._starting_cash, strict=strict, anomalies=anomalies
        )
        return result, raw
