from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from gkr_trading.backtest.execution_simulator import simulate_immediate_fill
from gkr_trading.core.events.builders import (
    market_bar,
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
from gkr_trading.core.risk import RiskEngine, RiskLimits
from gkr_trading.core.schemas.enums import Timeframe
from gkr_trading.core.schemas.fixed_clock import FixedClock
from gkr_trading.core.schemas.ids import SessionId, new_order_id
from gkr_trading.core.sessions.manager import SessionManager
from gkr_trading.data.access_api.service import DataAccessAPI, HistoricalBarQuery
from gkr_trading.data.market_store.repository import BarRow
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.base import Strategy


def _parse_ts(ts: str) -> datetime:
    s = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
    return datetime.fromisoformat(s).astimezone(UTC)


def run_backtest(
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
) -> PortfolioState:
    q = HistoricalBarQuery(
        universe_name=universe_name,
        instrument_ids=None,
        timeframe=timeframe,
        start_ts_utc=start_ts,
        end_ts_utc=end_ts,
    )
    bars = api.fetch_bars(q)
    sm = SessionManager(store, session_id)
    clock = FixedClock(datetime(2024, 1, 1, tzinfo=UTC))
    risk = RiskEngine(risk_limits)
    state = PortfolioState.initial(starting_cash)

    sm.append(session_started(session_id, "backtest", start_ts))

    history: list[BarRow] = []
    for bar in bars:
        clock.set(_parse_ts(bar.bar_ts_utc))
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

        intent = strategy.on_bar(bar, tuple(history))
        history.append(bar)

        if intent is None:
            continue

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

        for fe in simulate_immediate_fill(
            order_id=oid,
            instrument_id=intent.instrument_id,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
            limit_price=intent.limit_price,
            fill_price=bar.close,
            bar_ts_utc=bar.bar_ts_utc,
            occurred_at_utc=bar.bar_ts_utc,
        ):
            sm.append(fe)
            state = apply_canonical_event(state, fe)

        pu = portfolio_updated(state, bar.bar_ts_utc)
        sm.append(pu)

    sm.append(session_stopped(session_id, end_ts, reason="backtest_complete"))
    return state
