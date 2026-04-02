"""End-of-run operator report for paper sessions (CLI / structured logging).

Includes a failure-aware report for partial runs (explicit failures, non-zero exit).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.types import EventType
from gkr_trading.core.portfolio import PortfolioState
from gkr_trading.core.replay.engine import ReplayResult
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.live.broker_sync import PaperSessionRecoveryReport

AdapterMode = Literal["mock", "alpaca", "dry_run"]
RunStatus = Literal["success", "failed"]

_UNRESOLVED_MARK = "SUBMIT_UNCERTAINTY_UNRESOLVED"


@dataclass
class PaperSessionOperatorReport:
    """Structured summary after a completed ``run_paper_session`` (source: events + recovery + replay)."""

    session_id: str
    adapter_mode: AdapterMode
    resumed_session: bool
    started_fresh: bool
    bars_processed: int
    orders_submitted: int
    broker_acks: int
    fills_applied: int
    order_cancels: int
    order_rejects: int
    broker_reject_reasons: list[dict[str, Any]]
    broker_rejects_preview: list[dict[str, Any]]
    recovery_ran: bool
    used_persisted_broker_state: bool
    startup_broker_facts_seen: int
    broker_facts_recovered: int
    rehydration_anomalies: list[str]
    anomalies_count: int
    anomaly_types: list[dict[str, Any]]
    uncertainty_events_count: int
    uncertainty_resolved: bool
    uncertainty_unresolved: bool
    pages_polled: int
    pages_polled_orders: int
    pages_polled_activities: int
    uncertainty_resolve_pages_max: int
    final_cash: Decimal
    final_positions: dict[str, Decimal]
    replay_consistency_hint: str
    uncertainty_resolution_log: tuple[str, ...] = field(default_factory=tuple)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "adapter_mode": self.adapter_mode,
            "resumed_session": self.resumed_session,
            "started_fresh": self.started_fresh,
            "bars_processed": self.bars_processed,
            "orders_submitted": self.orders_submitted,
            "broker_acks": self.broker_acks,
            "fills_applied": self.fills_applied,
            "order_cancels": self.order_cancels,
            "order_rejects": self.order_rejects,
            "broker_reject_reasons": list(self.broker_reject_reasons),
            "broker_rejects_preview": list(self.broker_rejects_preview),
            "recovery_ran": self.recovery_ran,
            "used_persisted_broker_state": self.used_persisted_broker_state,
            "startup_broker_facts_seen": self.startup_broker_facts_seen,
            "broker_facts_recovered": self.broker_facts_recovered,
            "rehydration_anomalies": list(self.rehydration_anomalies),
            "anomalies_count": self.anomalies_count,
            "anomaly_types": list(self.anomaly_types),
            "uncertainty_events_count": self.uncertainty_events_count,
            "uncertainty_resolved": self.uncertainty_resolved,
            "uncertainty_unresolved": self.uncertainty_unresolved,
            "pages_polled": self.pages_polled,
            "pages_polled_orders": self.pages_polled_orders,
            "pages_polled_activities": self.pages_polled_activities,
            "uncertainty_resolve_pages_max": self.uncertainty_resolve_pages_max,
            "final_cash": str(self.final_cash),
            "final_positions": {k: str(v) for k, v in self.final_positions.items()},
            "replay_consistency_hint": self.replay_consistency_hint,
            "uncertainty_resolution_log": list(self.uncertainty_resolution_log),
        }

@dataclass
class PaperSessionFailureReport:
    """Structured summary for a failed/partial paper session run.

    Shape stays close to ``PaperSessionOperatorReport`` but allows unknowns.
    """

    status: RunStatus
    failure_type: str
    failure_message: str
    failure_phase: str | None

    session_id: str
    adapter_mode: AdapterMode | None
    resumed_session: bool | None
    started_fresh: bool | None

    startup_recovery_ran: bool | None
    startup_recovery_completed: bool | None
    used_persisted_broker_state: bool | None
    startup_broker_facts_seen: int | None
    broker_facts_recovered: int | None
    pages_polled: int | None
    pages_polled_orders: int | None
    pages_polled_activities: int | None

    uncertainty_events_count: int | None
    uncertainty_resolved: bool | None
    uncertainty_unresolved: bool | None
    uncertainty_resolve_pages_max: int | None

    bars_processed: int | None
    orders_submitted: int | None
    broker_acks: int | None
    fills_applied: int | None
    order_cancels: int | None
    order_rejects: int | None
    broker_reject_reasons: list[dict[str, Any]] | None
    broker_rejects_preview: list[dict[str, Any]] | None

    final_cash: Decimal | None
    final_positions: dict[str, Decimal] | None

    anomalies_count: int | None
    anomaly_types: list[dict[str, Any]] | None
    replay_consistency_hint: str | None

    rehydration_anomalies: list[str] = field(default_factory=list)
    uncertainty_resolution_log: tuple[str, ...] = field(default_factory=tuple)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "failure_type": self.failure_type,
            "failure_message": self.failure_message,
            "failure_phase": self.failure_phase,
            "session_id": self.session_id,
            "adapter_mode": self.adapter_mode,
            "resumed_session": self.resumed_session,
            "started_fresh": self.started_fresh,
            "startup_recovery_ran": self.startup_recovery_ran,
            "startup_recovery_completed": self.startup_recovery_completed,
            "used_persisted_broker_state": self.used_persisted_broker_state,
            "startup_broker_facts_seen": self.startup_broker_facts_seen,
            "broker_facts_recovered": self.broker_facts_recovered,
            "pages_polled": self.pages_polled,
            "pages_polled_orders": self.pages_polled_orders,
            "pages_polled_activities": self.pages_polled_activities,
            "uncertainty_events_count": self.uncertainty_events_count,
            "uncertainty_resolved": self.uncertainty_resolved,
            "uncertainty_unresolved": self.uncertainty_unresolved,
            "uncertainty_resolve_pages_max": self.uncertainty_resolve_pages_max,
            "bars_processed": self.bars_processed,
            "orders_submitted": self.orders_submitted,
            "broker_acks": self.broker_acks,
            "fills_applied": self.fills_applied,
            "order_cancels": self.order_cancels,
            "order_rejects": self.order_rejects,
            "broker_reject_reasons": (
                list(self.broker_reject_reasons) if self.broker_reject_reasons is not None else None
            ),
            "broker_rejects_preview": (
                list(self.broker_rejects_preview) if self.broker_rejects_preview is not None else None
            ),
            "final_cash": str(self.final_cash) if self.final_cash is not None else None,
            "final_positions": (
                {k: str(v) for k, v in self.final_positions.items()}
                if self.final_positions is not None
                else None
            ),
            "anomalies_count": self.anomalies_count,
            "anomaly_types": list(self.anomaly_types) if self.anomaly_types is not None else None,
            "replay_consistency_hint": self.replay_consistency_hint,
            "rehydration_anomalies": list(self.rehydration_anomalies),
            "uncertainty_resolution_log": list(self.uncertainty_resolution_log),
        }


@dataclass(frozen=True)
class PaperSessionRunResult:
    state: PortfolioState
    report: PaperSessionOperatorReport


@dataclass
class PaperSessionRunFailed(Exception):
    """Raised by ``run_paper_session`` with an attached operator failure report."""

    report: PaperSessionFailureReport



def _count_events_by_type(events: list[CanonicalEvent]) -> dict[EventType, int]:
    c: dict[EventType, int] = {}
    for e in events:
        c[e.event_type] = c.get(e.event_type, 0) + 1
    return c


def _anomaly_type_histogram(replay: ReplayResult, *, top_n: int = 12) -> list[dict[str, Any]]:
    codes = [a.code for a in replay.anomalies]
    ctr = Counter(codes)
    ranked = ctr.most_common(top_n)
    return [{"code": code, "count": n} for code, n in ranked]

def _broker_rejects_summary(
    events: list[CanonicalEvent],
    *,
    top_n: int = 8,
    preview_n: int = 10,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rej = [e for e in events if e.event_type == EventType.ORDER_REJECTED]
    ctr = Counter((getattr(e.payload, "reason_code", None) or "UNKNOWN") for e in rej)
    reasons = [{"reason_code": c, "count": n} for c, n in ctr.most_common(top_n)]
    preview: list[dict[str, Any]] = []
    for e in rej[:preview_n]:
        preview.append(
            {
                "order_id": str(getattr(e.payload, "order_id", "")),
                "reason_code": getattr(e.payload, "reason_code", None),
                "reason_detail": getattr(e.payload, "reason_detail", None),
            }
        )
    return reasons, preview


def _uncertainty_resolve_pages_max(log: tuple[str, ...]) -> int:
    m = 0
    for line in log:
        # "resolve_pages=N "
        key = "resolve_pages="
        if key not in line:
            continue
        try:
            start = line.index(key) + len(key)
            end = line.find(" ", start)
            chunk = line[start:end] if end != -1 else line[start:]
            m = max(m, int(chunk))
        except (ValueError, IndexError):
            continue
    return m


def build_paper_session_failure_report(
    *,
    session_id: SessionId,
    adapter_mode: AdapterMode | None,
    resumed_session: bool | None,
    bars_processed: int | None,
    events: list[CanonicalEvent] | None,
    state: PortfolioState | None,
    recovery: PaperSessionRecoveryReport | None,
    replay: ReplayResult | None,
    startup_recovery_ran: bool | None,
    startup_recovery_completed: bool | None,
    failure: Exception,
    failure_phase: str | None,
) -> PaperSessionFailureReport:
    evs = events or []
    counts = _count_events_by_type(evs) if events is not None else {}
    broker_reject_reasons, broker_rejects_preview = (
        _broker_rejects_summary(evs) if events is not None else (None, None)
    )

    order_pages = recovery.cumulative_pagination_order_pages if recovery is not None else None
    act_pages = recovery.cumulative_pagination_activity_pages if recovery is not None else None
    pages_polled = (order_pages + act_pages) if (order_pages is not None and act_pages is not None) else None

    ulog = tuple(recovery.uncertainty_resolution_log) if recovery is not None else tuple()
    uncertainty_resolve_pages_max = _uncertainty_resolve_pages_max(ulog) if recovery is not None else None
    uncertainty_resolved = any("found=True" in line for line in ulog) if recovery is not None else None
    uncertainty_unresolved = (
        any(_UNRESOLVED_MARK in a for a in recovery.rehydration_anomalies) if recovery is not None else None
    )

    n_anom = len(replay.anomalies) if replay is not None else None
    hint = ("ok" if n_anom == 0 else f"anomalies_{n_anom}") if n_anom is not None else None

    return PaperSessionFailureReport(
        status="failed",
        failure_type=type(failure).__name__,
        failure_message=str(failure)[:2000],
        failure_phase=failure_phase,
        session_id=str(session_id),
        adapter_mode=adapter_mode,
        resumed_session=resumed_session,
        started_fresh=(not resumed_session) if resumed_session is not None else None,
        startup_recovery_ran=startup_recovery_ran,
        startup_recovery_completed=startup_recovery_completed,
        used_persisted_broker_state=(
            recovery.used_persisted_broker_state if recovery is not None else None
        ),
        startup_broker_facts_seen=(
            recovery.startup_broker_facts_seen if recovery is not None else None
        ),
        broker_facts_recovered=(
            recovery.startup_broker_facts_seen if recovery is not None else None
        ),
        pages_polled=pages_polled,
        pages_polled_orders=order_pages,
        pages_polled_activities=act_pages,
        uncertainty_events_count=(len(ulog) if recovery is not None else None),
        uncertainty_resolved=uncertainty_resolved,
        uncertainty_unresolved=uncertainty_unresolved,
        uncertainty_resolve_pages_max=uncertainty_resolve_pages_max,
        bars_processed=bars_processed,
        orders_submitted=counts.get(EventType.ORDER_SUBMITTED) if events is not None else None,
        broker_acks=counts.get(EventType.ORDER_ACKNOWLEDGED) if events is not None else None,
        fills_applied=counts.get(EventType.FILL_RECEIVED) if events is not None else None,
        order_cancels=counts.get(EventType.ORDER_CANCELLED) if events is not None else None,
        order_rejects=counts.get(EventType.ORDER_REJECTED) if events is not None else None,
        broker_reject_reasons=broker_reject_reasons,
        broker_rejects_preview=broker_rejects_preview,
        final_cash=(state.cash if state is not None else None),
        final_positions=(dict(state.positions) if state is not None else None),
        anomalies_count=n_anom,
        anomaly_types=_anomaly_type_histogram(replay) if replay is not None else None,
        replay_consistency_hint=hint,
        rehydration_anomalies=list(recovery.rehydration_anomalies) if recovery is not None else [],
        uncertainty_resolution_log=ulog,
    )


def build_paper_session_operator_report(
    *,
    session_id: SessionId,
    adapter_mode: AdapterMode,
    resumed_session: bool,
    bars_processed: int,
    events: list[CanonicalEvent],
    state: PortfolioState,
    recovery: PaperSessionRecoveryReport,
    replay: ReplayResult,
) -> PaperSessionOperatorReport:
    counts = _count_events_by_type(events)
    broker_reject_reasons, broker_rejects_preview = _broker_rejects_summary(events)
    order_pages = recovery.cumulative_pagination_order_pages
    act_pages = recovery.cumulative_pagination_activity_pages
    ulog = tuple(recovery.uncertainty_resolution_log)
    uncertainty_resolved = any("found=True" in line for line in ulog)
    uncertainty_unresolved = any(_UNRESOLVED_MARK in a for a in recovery.rehydration_anomalies)
    n_anom = len(replay.anomalies)
    hint = "ok" if n_anom == 0 else f"anomalies_{n_anom}"

    return PaperSessionOperatorReport(
        session_id=str(session_id),
        adapter_mode=adapter_mode,
        resumed_session=resumed_session,
        started_fresh=not resumed_session,
        bars_processed=bars_processed,
        orders_submitted=counts.get(EventType.ORDER_SUBMITTED, 0),
        broker_acks=counts.get(EventType.ORDER_ACKNOWLEDGED, 0),
        fills_applied=counts.get(EventType.FILL_RECEIVED, 0),
        order_cancels=counts.get(EventType.ORDER_CANCELLED, 0),
        order_rejects=counts.get(EventType.ORDER_REJECTED, 0),
        broker_reject_reasons=broker_reject_reasons,
        broker_rejects_preview=broker_rejects_preview,
        recovery_ran=True,
        used_persisted_broker_state=recovery.used_persisted_broker_state,
        startup_broker_facts_seen=recovery.startup_broker_facts_seen,
        broker_facts_recovered=recovery.startup_broker_facts_seen,
        rehydration_anomalies=list(recovery.rehydration_anomalies),
        anomalies_count=n_anom,
        anomaly_types=_anomaly_type_histogram(replay),
        uncertainty_events_count=len(ulog),
        uncertainty_resolved=uncertainty_resolved,
        uncertainty_unresolved=uncertainty_unresolved,
        pages_polled=order_pages + act_pages,
        pages_polled_orders=order_pages,
        pages_polled_activities=act_pages,
        uncertainty_resolve_pages_max=_uncertainty_resolve_pages_max(ulog),
        final_cash=state.cash,
        final_positions=dict(state.positions),
        replay_consistency_hint=hint,
        uncertainty_resolution_log=ulog,
    )
