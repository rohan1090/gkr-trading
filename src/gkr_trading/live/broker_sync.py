"""
Bar-stepped broker ingestion: translate normalized BrokerFact rows into canonical events.

Within-sync ordering (see docs/ORCHESTRATOR_SPEC.md):
  1) rejects  2) cancels  3) fills
  Tie-break: occurred_at_utc, then tie_breaker() (execution id or order id).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from gkr_trading.core.events.builders import (
    fill_received,
    order_cancelled,
    order_rejected,
)
from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.types import EventType
from gkr_trading.core.portfolio import PortfolioState, apply_canonical_event
from gkr_trading.core.sessions.manager import SessionManager
from gkr_trading.live.broker_adapter import (
    BrokerAdapter,
    BrokerFillFact,
    BrokerFact,
    BrokerOrderCancelledFact,
    BrokerOrderRejectedFact,
    BrokerPollHints,
    BrokerPollResult,
    BrokerReconciliationCursor,
    BrokerSyncPhase,
)


_KIND_PRIORITY = {"reject": 0, "cancel": 1, "fill": 2}


@dataclass
class PaperSessionRecoveryReport:
    """Operator-visible paper/Alpaca recovery metrics (narrow, mutable)."""

    used_persisted_broker_state: bool = False
    rehydration_anomalies: list[str] = field(default_factory=list)
    startup_broker_facts_seen: int = 0
    cumulative_pagination_order_pages: int = 0
    cumulative_pagination_activity_pages: int = 0
    uncertainty_resolution_log: list[str] = field(default_factory=list)


@dataclass
class PaperBrokerSessionContext:
    """Reconciliation cursor, last poll batch, and recovery telemetry."""

    reconciliation_cursor: BrokerReconciliationCursor | None = None
    last_poll_batch: BrokerPollResult | None = None
    recovery: PaperSessionRecoveryReport = field(default_factory=PaperSessionRecoveryReport)


def seen_broker_execution_ids_from_events(events: list[CanonicalEvent]) -> set[str]:
    """Rebuild dedupe set from persisted session (restart / idempotent reconciliation)."""
    out: set[str] = set()
    for e in events:
        if e.event_type != EventType.FILL_RECEIVED:
            continue
        bex = getattr(e.payload, "broker_execution_id", None)
        if bex:
            out.add(str(bex))
    return out


def sort_broker_facts_for_append(facts: tuple[BrokerFact, ...]) -> list[BrokerFact]:
    return sorted(
        facts,
        key=lambda f: (_KIND_PRIORITY[f.kind], f.occurred_at_utc, f.tie_breaker()),
    )


def broker_fact_to_canonical(fact: BrokerFact) -> CanonicalEvent:
    if isinstance(fact, BrokerOrderRejectedFact):
        return order_rejected(
            fact.client_order_id,
            fact.reason_code,
            fact.occurred_at_utc,
            reason_detail=fact.reason_detail,
        )
    if isinstance(fact, BrokerOrderCancelledFact):
        return order_cancelled(
            fact.client_order_id,
            fact.occurred_at_utc,
            reason_code=fact.reason_code,
            cancelled_qty=fact.cancelled_qty,
        )
    if isinstance(fact, BrokerFillFact):
        return fill_received(
            fact.client_order_id,
            fact.instrument_id,
            fact.side,
            fact.quantity,
            fact.price,
            fact.fill_ts_utc,
            fact.occurred_at_utc,
            fees=fact.fees,
            broker_execution_id=fact.broker_execution_id,
        )
    raise TypeError(f"unknown BrokerFact: {type(fact)!r}")


def append_sorted_broker_facts(
    sm: SessionManager,
    state: PortfolioState,
    facts: tuple[BrokerFact, ...],
    *,
    seen_broker_execution_ids: set[str],
) -> PortfolioState:
    """Append canonical events for facts in deterministic order; skip duplicate execution ids."""
    for fact in sort_broker_facts_for_append(facts):
        if isinstance(fact, BrokerFillFact):
            if fact.broker_execution_id in seen_broker_execution_ids:
                continue
            seen_broker_execution_ids.add(fact.broker_execution_id)
        ev = broker_fact_to_canonical(fact)
        sm.append(ev)
        state = apply_canonical_event(state, ev)
    return state


def run_broker_sync_phase(
    brk: BrokerAdapter,
    *,
    sm: SessionManager,
    state: PortfolioState,
    session_ctx: PaperBrokerSessionContext | None,
    hints: BrokerPollHints,
    phase: BrokerSyncPhase,
    seen_broker_execution_ids: set[str],
) -> PortfolioState:
    """Poll adapter, append+fold sorted facts, advance session_ctx.reconciliation_cursor."""
    cur = session_ctx.reconciliation_cursor if session_ctx else None
    batch: BrokerPollResult = brk.poll_broker_facts(cursor=cur, hints=hints, phase=phase)
    if session_ctx is not None:
        session_ctx.reconciliation_cursor = batch.cursor
        session_ctx.last_poll_batch = batch
        if phase == BrokerSyncPhase.STARTUP:
            session_ctx.recovery.startup_broker_facts_seen += len(batch.facts)
    return append_sorted_broker_facts(
        sm,
        state,
        batch.facts,
        seen_broker_execution_ids=seen_broker_execution_ids,
    )
