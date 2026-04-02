from __future__ import annotations

from datetime import time
from decimal import Decimal

from gkr_trading.backtest.orchestrator import run_backtest
from gkr_trading.cli import seed
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import Timeframe
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.data.access_api.service import DataAccessAPI
from gkr_trading.persistence.db import open_sqlite
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.sample_strategy import SampleBarCrossStrategy


def test_backtest_persists_events_and_updates_portfolio(tmp_path) -> None:
    db = str(tmp_path / "x.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000aa01")
    limits = RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )
    state = run_backtest(
        api=api,
        store=store,
        session_id=sid,
        strategy=SampleBarCrossStrategy(trade_qty=Decimal("10")),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=limits,
    )
    evs = store.load_session(str(sid))
    conn.close()
    assert len(evs) > 5
    assert state.cash < Decimal("100000") or state.positions  # bought on signal
    assert any(e.event_type.value == "fill_received" for e in evs)
