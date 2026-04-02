from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

from gkr_trading.core.events.types import EventType
from gkr_trading.core.events.builders import (
    market_bar,
    order_acknowledged,
    order_rejected,
    order_submitted,
    portfolio_updated,
    risk_approved,
    risk_rejected,
    session_started,
    session_stopped,
    signal_generated,
    trade_intent_created,
)
from gkr_trading.core.portfolio import PortfolioState, apply_canonical_event
from gkr_trading.core.replay.engine import ReplayEngine
from gkr_trading.core.risk import RiskEngine, RiskLimits
from gkr_trading.core.schemas.enums import Timeframe
from gkr_trading.core.schemas.fixed_clock import FixedClock
from gkr_trading.core.schemas.ids import InstrumentId, SessionId, new_order_id
from gkr_trading.core.sessions.manager import SessionManager
from gkr_trading.data.access_api.service import DataAccessAPI, HistoricalBarQuery
from gkr_trading.data.market_store.repository import BarRow
from gkr_trading.live.broker_adapter import (
    BrokerAdapter,
    BrokerPollHints,
    BrokerSyncPhase,
    MockBrokerAdapter,
    SubmitRequest,
)
from gkr_trading.live.alpaca_http import (
    AlpacaSubmitUncertaintyError,
    AlpacaSubmitUnresolvedError,
)
from gkr_trading.live.alpaca_paper_adapter import AlpacaPaperAdapter
from gkr_trading.live.alpaca_rehydrate import rehydrate_tracked_orders_from_events
from gkr_trading.live.broker_sync import (
    PaperBrokerSessionContext,
    run_broker_sync_phase,
    seen_broker_execution_ids_from_events,
)
from gkr_trading.live.paper_session_report import (
    AdapterMode,
    PaperSessionRunResult,
    PaperSessionRunFailed,
    build_paper_session_failure_report,
    build_paper_session_operator_report,
)
from gkr_trading.persistence.broker_reconciliation_store import (
    load_broker_reconciliation_payload,
    save_broker_reconciliation_payload,
)
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.base import Strategy


def _parse_ts(ts: str) -> datetime:
    s = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
    return datetime.fromisoformat(s).astimezone(UTC)


def _session_log_contains_started(events: list) -> bool:
    return any(e.event_type == EventType.SESSION_STARTED for e in events)


def _alpaca_load_and_rehydrate(
    brk: BrokerAdapter,
    *,
    conn,
    session_id: SessionId,
    pre_existing: list,
    starting_cash: Decimal,
    seen_exec_ids: set[str],
    recovery,
) -> None:
    if not isinstance(brk, AlpacaPaperAdapter):
        return
    blob = load_broker_reconciliation_payload(conn, str(session_id))
    if blob:
        brk.import_persisted_payload(blob)
        recovery.used_persisted_broker_state = True
    by_c, alp, an = rehydrate_tracked_orders_from_events(pre_existing, starting_cash)
    for msg in an:
        recovery.rehydration_anomalies.append(msg)
    brk.merge_rehydrated_tracked(by_c, alp, union_fill_emitted=set(seen_exec_ids))


def _alpaca_after_sync(
    brk: BrokerAdapter,
    *,
    conn,
    session_id: SessionId,
    recovery,
) -> None:
    if not isinstance(brk, AlpacaPaperAdapter):
        return
    op, ap = brk.last_poll_page_counts()
    recovery.cumulative_pagination_order_pages += op
    recovery.cumulative_pagination_activity_pages += ap
    save_broker_reconciliation_payload(conn, str(session_id), brk.export_persisted_payload())


def run_paper_session(
    *,
    api: DataAccessAPI,
    store: SqliteEventStore,
    session_id: SessionId,
    strategy: Strategy,
    universe_name: str,
    timeframe: Timeframe,
    start_ts: str,
    end_ts: str,
    starting_cash: Decimal,
    risk_limits: RiskLimits,
    broker: BrokerAdapter | None = None,
    symbol_resolver: Callable[[InstrumentId], str] | None = None,
    broker_session: PaperBrokerSessionContext | None = None,
    resume_existing_session: bool = False,
    dry_run: bool = False,
) -> PaperSessionRunResult:
    """
    Paper path: bar-stepped broker sync fences + append-only canonical events.

    When ``symbol_resolver`` is set, it must return the executable broker symbol for
    ``instrument_id`` (e.g. Alpaca equity ticker); resolution runs before submit.

    ``resume_existing_session``: if True and the event log already contains
    ``SESSION_STARTED`` for this ``session_id``, skip appending another session start
    (pragmatic resume; does not rewrite history).

    ``dry_run``: report ``adapter_mode=dry_run`` (orchestration path only; use with mock broker,
    no live broker orders).

    Returns folded ``PortfolioState`` plus ``PaperSessionOperatorReport`` for CLI/operators.
    """
    brk: BrokerAdapter | None = None
    sm: SessionManager | None = None
    bctx: PaperBrokerSessionContext | None = None
    pre_existing: list | None = None
    resumed_session: bool | None = None
    bars_processed: int | None = None
    state: PortfolioState | None = None
    adapter_mode: AdapterMode | None = None
    startup_recovery_ran: bool | None = False
    startup_recovery_completed: bool | None = False
    failure_phase: str | None = None

    try:
        failure_phase = "fetch_bars"
        q = HistoricalBarQuery(
            universe_name=universe_name,
            instrument_ids=None,
            timeframe=timeframe,
            start_ts_utc=start_ts,
            end_ts_utc=end_ts,
        )
        bars = api.fetch_bars(q)

        brk = broker or MockBrokerAdapter()
        sm = SessionManager(store, session_id)
        clock = FixedClock(datetime(2024, 1, 1, tzinfo=UTC))
        risk = RiskEngine(risk_limits)
        state = PortfolioState.initial(starting_cash)
        bctx = broker_session or PaperBrokerSessionContext()

        if isinstance(brk, AlpacaPaperAdapter):
            adapter_mode = "alpaca"
        elif dry_run:
            adapter_mode = "dry_run"
        else:
            adapter_mode = "mock"

        failure_phase = "load_pre_existing"
        pre_existing = store.load_session(str(session_id))
        seen_exec_ids = seen_broker_execution_ids_from_events(pre_existing)
        resumed_session = resume_existing_session and _session_log_contains_started(pre_existing)

        failure_phase = "alpaca_load_and_rehydrate"
        _alpaca_load_and_rehydrate(
            brk,
            conn=store._conn,
            session_id=session_id,
            pre_existing=pre_existing,
            starting_cash=starting_cash,
            seen_exec_ids=seen_exec_ids,
            recovery=bctx.recovery,
        )

        failure_phase = "session_started_append"
        if resumed_session:
            pass
        else:
            sm.append(session_started(session_id, "paper", start_ts))

        failure_phase = "startup_sync"
        startup_recovery_ran = True
        startup_hints = BrokerPollHints(
            bar_ts_utc=start_ts,
            reference_price=None,
            default_occurred_at_utc=start_ts,
        )
        state = run_broker_sync_phase(
            brk,
            sm=sm,
            state=state,
            session_ctx=bctx,
            hints=startup_hints,
            phase=BrokerSyncPhase.STARTUP,
            seen_broker_execution_ids=seen_exec_ids,
        )
        _alpaca_after_sync(brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery)
        startup_recovery_completed = True

        history: list[BarRow] = []
        bars_processed = 0
        for bar in bars:
            bars_processed += 1

            failure_phase = "pre_bar_sync"
            clock.set(_parse_ts(bar.bar_ts_utc))
            hints = BrokerPollHints(
                bar_ts_utc=bar.bar_ts_utc,
                reference_price=bar.close,
                default_occurred_at_utc=bar.bar_ts_utc,
            )

            state = run_broker_sync_phase(
                brk,
                sm=sm,
                state=state,
                session_ctx=bctx,
                hints=hints,
                phase=BrokerSyncPhase.PRE_BAR,
                seen_broker_execution_ids=seen_exec_ids,
            )
            _alpaca_after_sync(brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery)

            mb = market_bar(
                bar.instrument_id,
                bar.timeframe,
                bar.bar_ts_utc,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.bar_ts_utc,
            )
            sm.append(mb)
            state = apply_canonical_event(state, mb)

            failure_phase = "strategy_on_bar"
            intent = strategy.on_bar(bar, tuple(history))
            history.append(bar)

            if intent is None:
                failure_phase = "post_bar_sync_no_intent"
                state = run_broker_sync_phase(
                    brk,
                    sm=sm,
                    state=state,
                    session_ctx=bctx,
                    hints=hints,
                    phase=BrokerSyncPhase.POST_BAR,
                    seen_broker_execution_ids=seen_exec_ids,
                )
                _alpaca_after_sync(
                    brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery
                )
                continue

            failure_phase = "intent_append"
            sm.append(
                signal_generated(
                    strategy.name,
                    bar.instrument_id,
                    "bar_pattern",
                    bar.bar_ts_utc,
                )
            )
            sm.append(trade_intent_created(intent, bar.bar_ts_utc))

            oid = new_order_id()
            failure_phase = "risk_evaluate"
            res = risk.evaluate(intent, state, clock, bar.close, oid)
            if not res.approved:
                rr = risk_rejected(
                    intent.intent_id,
                    res.reason_code or "UNKNOWN",
                    res.reason_detail,
                    bar.bar_ts_utc,
                )
                sm.append(rr)
                state = apply_canonical_event(state, rr)
                failure_phase = "post_bar_sync_risk_reject"
                state = run_broker_sync_phase(
                    brk,
                    sm=sm,
                    state=state,
                    session_ctx=bctx,
                    hints=hints,
                    phase=BrokerSyncPhase.POST_BAR,
                    seen_broker_execution_ids=seen_exec_ids,
                )
                _alpaca_after_sync(
                    brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery
                )
                continue

            sm.append(risk_approved(intent.intent_id, oid, bar.bar_ts_utc))
            os_ev = order_submitted(
                oid,
                intent.instrument_id,
                intent.side,
                intent.quantity,
                intent.order_type,
                intent.limit_price,
                bar.bar_ts_utc,
            )
            sm.append(os_ev)
            state = apply_canonical_event(state, os_ev)

            exe_sym: str | None = None
            if symbol_resolver is not None:
                exe_sym = symbol_resolver(intent.instrument_id)

            submit_req = SubmitRequest(
                order_id=oid,
                instrument_id=intent.instrument_id,
                side=intent.side,
                quantity=intent.quantity,
                order_type=intent.order_type,
                limit_price=intent.limit_price,
                executable_broker_symbol=exe_sym,
                context_ts_utc=bar.bar_ts_utc,
            )
            try:
                failure_phase = "broker_submit"
                sub = brk.submit(submit_req)
            except AlpacaSubmitUncertaintyError:
                if isinstance(brk, AlpacaPaperAdapter):
                    failure_phase = "submit_uncertainty_resolve"
                    rslv = brk.resolve_submit_uncertainty(submit_req)
                    bctx.recovery.uncertainty_resolution_log.append(
                        f"client_order_id={oid} resolve_pages={brk.last_uncertainty_resolve_pages()} "
                        f"found={rslv.found} detail={rslv.detail!r}"
                    )
                    if rslv.found and rslv.submission_result is not None:
                        sub = rslv.submission_result
                    else:
                        bctx.recovery.rehydration_anomalies.append(
                            f"SUBMIT_UNCERTAINTY_UNRESOLVED: order_id={oid} {rslv.detail or ''}"
                        )
                        raise AlpacaSubmitUnresolvedError(
                            "Submit uncertain and broker lookup found no order for "
                            f"client_order_id={oid}",
                            client_order_id=str(oid),
                        ) from None
                else:
                    raise
            if sub.rejected:
                failure_phase = "broker_reject_append"
                rej = order_rejected(
                    oid,
                    sub.reject_reason_code or "BROKER_REJECT",
                    sub.occurred_at_utc,
                    reason_detail=sub.reject_reason_detail,
                )
                sm.append(rej)
                state = apply_canonical_event(state, rej)
                failure_phase = "post_submit_sync_reject"
                state = run_broker_sync_phase(
                    brk,
                    sm=sm,
                    state=state,
                    session_ctx=bctx,
                    hints=hints,
                    phase=BrokerSyncPhase.POST_SUBMIT,
                    seen_broker_execution_ids=seen_exec_ids,
                )
                _alpaca_after_sync(
                    brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery
                )
                failure_phase = "post_bar_sync_after_reject"
                state = run_broker_sync_phase(
                    brk,
                    sm=sm,
                    state=state,
                    session_ctx=bctx,
                    hints=hints,
                    phase=BrokerSyncPhase.POST_BAR,
                    seen_broker_execution_ids=seen_exec_ids,
                )
                _alpaca_after_sync(
                    brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery
                )
                continue

            failure_phase = "ack_append"
            ack = order_acknowledged(
                oid,
                sub.occurred_at_utc,
                broker_order_id=sub.broker_order_id,
            )
            sm.append(ack)
            state = apply_canonical_event(state, ack)

            failure_phase = "post_submit_sync"
            state = run_broker_sync_phase(
                brk,
                sm=sm,
                state=state,
                session_ctx=bctx,
                hints=hints,
                phase=BrokerSyncPhase.POST_SUBMIT,
                seen_broker_execution_ids=seen_exec_ids,
            )
            _alpaca_after_sync(
                brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery
            )

            failure_phase = "post_bar_sync"
            state = run_broker_sync_phase(
                brk,
                sm=sm,
                state=state,
                session_ctx=bctx,
                hints=hints,
                phase=BrokerSyncPhase.POST_BAR,
                seen_broker_execution_ids=seen_exec_ids,
            )
            _alpaca_after_sync(
                brk, conn=store._conn, session_id=session_id, recovery=bctx.recovery
            )

            sm.append(portfolio_updated(state, bar.bar_ts_utc))

        failure_phase = "session_stopped_append"
        sm.append(session_stopped(session_id, end_ts, reason="paper_complete"))

        final_events = store.load_session(str(session_id))
        eng = ReplayEngine(store, starting_cash)
        replay_res, _ = eng.replay_session(session_id, strict=False)
        report = build_paper_session_operator_report(
            session_id=session_id,
            adapter_mode=adapter_mode or "mock",
            resumed_session=bool(resumed_session),
            bars_processed=int(bars_processed or 0),
            events=final_events,
            state=state,
            recovery=bctx.recovery,
            replay=replay_res,
        )
        return PaperSessionRunResult(state=state, report=report)
    except Exception as e:
        # Best-effort: read persisted canonical events and run replay on what exists so far.
        try:
            evs = store.load_session(str(session_id))
        except Exception:
            evs = None
        try:
            eng = ReplayEngine(store, starting_cash)
            replay_res, _ = eng.replay_session(session_id, strict=False)
        except Exception:
            replay_res = None

        recovery = bctx.recovery if bctx is not None else None
        rep = build_paper_session_failure_report(
            session_id=session_id,
            adapter_mode=adapter_mode,
            resumed_session=resumed_session,
            bars_processed=bars_processed,
            events=evs,
            state=state,
            recovery=recovery,
            replay=replay_res,
            startup_recovery_ran=startup_recovery_ran,
            startup_recovery_completed=startup_recovery_completed,
            failure=e,
            failure_phase=failure_phase,
        )
        raise PaperSessionRunFailed(rep) from e
