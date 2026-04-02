from __future__ import annotations

from datetime import time
from decimal import Decimal

from gkr_trading.backtest.orchestrator import run_backtest
from gkr_trading.cli import seed
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import Timeframe
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.data.access_api.service import DataAccessAPI
from gkr_trading.live.broker_adapter import MockBrokerAdapter
from gkr_trading.live.runtime import run_paper_session
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.sample_strategy import SampleBarCrossStrategy


def test_backtest_and_paper_match_portfolio_economics(tmp_path) -> None:
    """Same normalized fills and marks → same cash/positions/realized (order ids differ)."""
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

    db_b = str(tmp_path / "b.db")
    conn_b = seed.initialize_database(db_b)
    seed.seed_instruments(conn_b)
    seed.seed_equity_bars(conn_b)
    store_b = SqliteEventStore(conn_b)
    api_b = DataAccessAPI(conn_b)
    sid_b = SessionId("00000000-0000-4000-8000-00000000cc01")
    st_b = run_backtest(
        api=api_b,
        store=store_b,
        session_id=sid_b,
        strategy=strat,
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts=start,
        end_ts=end,
        starting_cash=Decimal("100000"),
        risk_limits=limits,
    )
    conn_b.close()

    db_p = str(tmp_path / "p.db")
    conn_p = seed.initialize_database(db_p)
    seed.seed_instruments(conn_p)
    seed.seed_equity_bars(conn_p)
    store_p = SqliteEventStore(conn_p)
    api_p = DataAccessAPI(conn_p)
    sid_p = SessionId("00000000-0000-4000-8000-00000000cc02")
    st_p = run_paper_session(
        api=api_p,
        store=store_p,
        session_id=sid_p,
        strategy=strat,
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts=start,
        end_ts=end,
        starting_cash=Decimal("100000"),
        risk_limits=limits,
        broker=MockBrokerAdapter(),
    ).state
    conn_p.close()

    assert st_b.cash == st_p.cash
    assert st_b.positions == st_p.positions
    assert st_b.realized_pnl == st_p.realized_pnl
    assert st_b.avg_entry == st_p.avg_entry
