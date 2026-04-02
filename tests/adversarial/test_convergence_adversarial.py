"""Scenarios 26–29: semantic parity backtest vs paper."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

from gkr_trading.backtest.execution_simulator import simulate_immediate_fill
from gkr_trading.backtest.orchestrator import run_backtest
from gkr_trading.cli import seed
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import OrderSide, OrderType, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId, OrderId, SessionId
from gkr_trading.data.access_api.service import DataAccessAPI
from gkr_trading.live.broker_adapter import MockBrokerAdapter
from gkr_trading.live.fill_handler import synthetic_fill
from gkr_trading.live.runtime import run_paper_session
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.sample_strategy import SampleBarCrossStrategy


def test_backtest_paper_fill_payload_contract_matches(tmp_path) -> None:
    """28 — same shape: full qty at bar close."""
    iid = InstrumentId("00000000-0000-4000-8000-000000000001")
    oid = OrderId("00000000-0000-4000-8000-00000003001")
    ts = "2024-01-01T15:00:00Z"
    sim = simulate_immediate_fill(
        order_id=oid,
        instrument_id=iid,
        side=OrderSide.BUY,
        quantity=Decimal("3"),
        order_type=OrderType.MARKET,
        limit_price=None,
        fill_price=Decimal("42"),
        bar_ts_utc=ts,
        occurred_at_utc=ts,
    )
    paper_fill = synthetic_fill(oid, iid, OrderSide.BUY, Decimal("3"), Decimal("42"), ts, ts)
    assert sim[-1].event_type == paper_fill.event_type
    sp = sim[-1].payload
    pp = paper_fill.payload
    assert sp.order_id == pp.order_id and sp.instrument_id == pp.instrument_id
    assert sp.side == pp.side and sp.fill_qty == pp.fill_qty and sp.fill_price == pp.fill_price
    assert sp.fill_ts_utc == pp.fill_ts_utc


def test_replay_parity_backtest_vs_paper_on_seed_db(tmp_path) -> None:
    """26, 29 — economics + replay output."""
    limits = RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )
    strat = SampleBarCrossStrategy(trade_qty=Decimal("10"))
    start = "2024-01-01T00:00:00Z"
    end = "2024-12-31T23:59:59Z"

    def run_one(which: str) -> tuple[object, list]:
        db = str(tmp_path / f"{which}.db")
        conn = seed.initialize_database(db)
        seed.seed_instruments(conn)
        seed.seed_equity_bars(conn)
        store = SqliteEventStore(conn)
        api = DataAccessAPI(conn)
        sid = SessionId(
            "00000000-0000-4000-8000-00000003aaa1"
            if which == "bt"
            else "00000000-0000-4000-8000-00000003bbb2"
        )
        if which == "bt":
            st = run_backtest(
                api=api,
                store=store,
                session_id=sid,
                strategy=strat,
                universe_name="demo",
                timeframe=Timeframe.D1,
                start_ts=start,
                end_ts=end,
                starting_cash=Decimal("100000"),
                risk_limits=limits,
            )
        else:
            st = run_paper_session(
                api=api,
                store=store,
                session_id=sid,
                strategy=strat,
                universe_name="demo",
                timeframe=Timeframe.D1,
                start_ts=start,
                end_ts=end,
                starting_cash=Decimal("100000"),
                risk_limits=limits,
                broker=MockBrokerAdapter(),
            ).state
        evs = store.load_session(str(sid))
        conn.close()
        return st, evs

    st_b, ev_b = run_one("bt")
    st_p, ev_p = run_one("pa")
    assert (st_b.cash, st_b.positions, st_b.realized_pnl) == (st_p.cash, st_p.positions, st_p.realized_pnl)

    from gkr_trading.core.replay.engine import replay_portfolio_state

    rb = replay_portfolio_state(ev_b, Decimal("100000")).state
    rp = replay_portfolio_state(ev_p, Decimal("100000")).state
    assert rb.cash == rp.cash and rb.positions == rp.positions


def test_event_count_may_differ_semantics_must_match(tmp_path) -> None:
    """27 — paper has broker submit side effect; both have fill_received for same trades."""
    limits = RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )
    db_b = str(tmp_path / "c1.db")
    db_p = str(tmp_path / "c2.db")
    for db in (db_b, db_p):
        conn = seed.initialize_database(db)
        seed.seed_instruments(conn)
        seed.seed_equity_bars(conn)
        conn.close()

    def count_fills(db: str) -> int:
        conn = __import__("gkr_trading.persistence.db", fromlist=["open_sqlite"]).open_sqlite(db)
        store = SqliteEventStore(conn)
        sid = SessionId("00000000-0000-4000-8000-00000004001")
        if "c1" in db:
            run_backtest(
                api=DataAccessAPI(conn),
                store=store,
                session_id=sid,
                strategy=SampleBarCrossStrategy(),
                universe_name="demo",
                timeframe=Timeframe.D1,
                start_ts="2024-01-01T00:00:00Z",
                end_ts="2024-12-31T23:59:59Z",
                starting_cash=Decimal("100000"),
                risk_limits=limits,
            )
        else:
            run_paper_session(
                api=DataAccessAPI(conn),
                store=store,
                session_id=sid,
                strategy=SampleBarCrossStrategy(),
                universe_name="demo",
                timeframe=Timeframe.D1,
                start_ts="2024-01-01T00:00:00Z",
                end_ts="2024-12-31T23:59:59Z",
                starting_cash=Decimal("100000"),
                risk_limits=limits,
                broker=MockBrokerAdapter(),
            )
        evs = store.load_session(str(sid))
        conn.close()
        return sum(1 for e in evs if e.event_type.value == "fill_received")

    assert count_fills(db_b) == count_fills(db_p)
