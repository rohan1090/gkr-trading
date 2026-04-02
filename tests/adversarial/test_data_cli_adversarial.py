"""Scenarios 30–38: identity, data plane, CLI-level behavior."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

from gkr_trading.backtest.orchestrator import run_backtest
from gkr_trading.cli import seed
from gkr_trading.core.replay import ReplayEngine
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import AssetClass, Timeframe
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.data.access_api.service import DataAccessAPI, HistoricalBarQuery
from gkr_trading.data.derived_views.scaffolding import NullDerivedViews
from gkr_trading.data.market_store.repository import MarketDataRepository
from gkr_trading.persistence.db import open_sqlite
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.sample_strategy import SampleBarCrossStrategy


def test_bar_rows_use_instrument_id_not_symbol_30(tmp_path) -> None:
    db = str(tmp_path / "id.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    api = DataAccessAPI(conn)
    q = HistoricalBarQuery(
        universe_name="demo",
        instrument_ids=None,
        timeframe=Timeframe.D1,
        start_ts_utc="2020-01-01T00:00:00Z",
        end_ts_utc="2030-01-01T00:00:00Z",
    )
    bars = api.fetch_bars(q)
    conn.close()
    assert bars
    assert all(str(b.instrument_id) == str(seed.DEMO_EQUITY_ID) for b in bars)


def test_universe_membership_31(tmp_path) -> None:
    db = str(tmp_path / "u.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    api = DataAccessAPI(conn)
    q = HistoricalBarQuery(
        universe_name="demo",
        instrument_ids=None,
        timeframe=Timeframe.D1,
        start_ts_utc="2020-01-01T00:00:00Z",
        end_ts_utc="2030-01-01T00:00:00Z",
    )
    ids = {str(b.instrument_id) for b in api.fetch_bars(q)}
    conn.close()
    assert str(seed.DEMO_EQUITY_ID) in ids


def test_futures_table_resolve_whitelist_32(tmp_path) -> None:
    db = str(tmp_path / "f.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    m = MarketDataRepository(conn)
    assert m.resolve_table_for_instrument(seed.DEMO_FUTURE_ID, AssetClass.FUTURE) == "futures_bars"
    conn.close()


def test_derived_views_not_required_for_replay_33() -> None:
    nv = NullDerivedViews()
    assert nv.series_instrument_at("X", "t") is None
    assert nv.snapshot(seed.DEMO_EQUITY_ID, "t") is None


def test_missing_session_replay_returns_initial_34_35(tmp_path) -> None:
    db = str(tmp_path / "m.db")
    conn = open_sqlite(db)
    conn.executescript(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            envelope_json TEXT NOT NULL,
            UNIQUE(session_id, seq)
        );
        """
    )
    store = SqliteEventStore(conn)
    eng = ReplayEngine(store, Decimal("50000"))
    result, raw = eng.replay_session(SessionId("00000000-0000-4000-8000-000000999991"))
    conn.close()
    assert raw == []
    assert result.state.cash == Decimal("50000")
    assert result.state.positions == {}


def test_portfolio_show_before_fills_36(tmp_path) -> None:
    db = str(tmp_path / "e.db")
    conn = open_sqlite(db)
    conn.executescript(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            envelope_json TEXT NOT NULL,
            UNIQUE(session_id, seq)
        );
        """
    )
    store = SqliteEventStore(conn)
    from gkr_trading.core.events.builders import session_started

    sid = SessionId("00000000-0000-4000-8000-000000888881")
    store.append(str(sid), session_started(sid, "paper", "2024-01-01T00:00:00Z"))
    eng = ReplayEngine(store, Decimal("777"))
    result, _ = eng.replay_session(sid)
    conn.close()
    assert result.state.cash == Decimal("777")


def test_empty_bar_store_backtest_no_crash_37(tmp_path) -> None:
    db = str(tmp_path / "empty.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    limits = RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )
    run_backtest(
        api=api,
        store=store,
        session_id=SessionId("00000000-0000-4000-8000-000000777771"),
        strategy=SampleBarCrossStrategy(),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=limits,
    )
    conn.close()


def test_seed_demo_ids_stable_38() -> None:
    assert str(seed.DEMO_EQUITY_ID) == "00000000-0000-4000-8000-000000000001"
    assert str(seed.DEMO_FUTURE_ID) == "00000000-0000-4000-8000-000000000002"
