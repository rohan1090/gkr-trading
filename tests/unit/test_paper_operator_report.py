"""Operator report construction and paper-session flags (deterministic, no network)."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

from typer.testing import CliRunner

from gkr_trading.cli import seed
from gkr_trading.cli.main import app
from gkr_trading.core.events.builders import session_started
from gkr_trading.core.events.types import EventType
from gkr_trading.core.portfolio import PortfolioState
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import OrderSide, OrderType, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId, OrderId, SessionId
from gkr_trading.core.sessions.manager import SessionManager
from gkr_trading.data.access_api.service import DataAccessAPI
from gkr_trading.live.broker_adapter import (
    BrokerFillFact,
    BrokerPollHints,
    BrokerSyncPhase,
    MockBrokerAdapter,
)
from gkr_trading.live.broker_sync import PaperSessionRecoveryReport
from gkr_trading.live.paper_session_report import (
    PaperSessionOperatorReport,
    build_paper_session_operator_report,
)
from gkr_trading.live.runtime import run_paper_session
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.sample_strategy import SampleBarCrossStrategy


def _risk() -> RiskLimits:
    return RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )


def test_to_jsonable_stable_keys() -> None:
    rep = PaperSessionOperatorReport(
        session_id="s1",
        adapter_mode="mock",
        resumed_session=False,
        started_fresh=True,
        bars_processed=1,
        orders_submitted=0,
        broker_acks=0,
        fills_applied=0,
        order_cancels=0,
        order_rejects=0,
        broker_reject_reasons=[],
        broker_rejects_preview=[],
        recovery_ran=True,
        used_persisted_broker_state=False,
        startup_broker_facts_seen=0,
        broker_facts_recovered=0,
        rehydration_anomalies=[],
        anomalies_count=0,
        anomaly_types=[],
        uncertainty_events_count=0,
        uncertainty_resolved=False,
        uncertainty_unresolved=False,
        pages_polled=0,
        pages_polled_orders=0,
        pages_polled_activities=0,
        uncertainty_resolve_pages_max=0,
        final_cash=Decimal("1"),
        final_positions={},
        replay_consistency_hint="ok",
    )
    keys = sorted(rep.to_jsonable().keys())
    assert keys == sorted(
        [
            "session_id",
            "adapter_mode",
            "resumed_session",
            "started_fresh",
            "bars_processed",
            "orders_submitted",
            "broker_acks",
            "fills_applied",
            "order_cancels",
            "order_rejects",
            "broker_reject_reasons",
            "broker_rejects_preview",
            "recovery_ran",
            "used_persisted_broker_state",
            "startup_broker_facts_seen",
            "broker_facts_recovered",
            "rehydration_anomalies",
            "anomalies_count",
            "anomaly_types",
            "uncertainty_events_count",
            "uncertainty_resolved",
            "uncertainty_unresolved",
            "pages_polled",
            "pages_polled_orders",
            "pages_polled_activities",
            "uncertainty_resolve_pages_max",
            "final_cash",
            "final_positions",
            "replay_consistency_hint",
            "uncertainty_resolution_log",
        ]
    )


def test_build_report_uncertainty_resolved_from_log() -> None:
    iid = InstrumentId("00000000-0000-4000-8000-000000000001")
    evs: list = []
    recovery = PaperSessionRecoveryReport()
    recovery.uncertainty_resolution_log.append(
        "client_order_id=x resolve_pages=2 found=True detail='ok'"
    )
    st = PortfolioState.initial(Decimal("100"))
    replay = replay_portfolio_state(evs, Decimal("100"))
    r = build_paper_session_operator_report(
        session_id=SessionId("00000000-0000-4000-8000-00000000a001"),
        adapter_mode="alpaca",
        resumed_session=False,
        bars_processed=0,
        events=evs,
        state=st,
        recovery=recovery,
        replay=replay,
    )
    assert r.uncertainty_resolved is True
    assert r.uncertainty_unresolved is False
    assert r.uncertainty_events_count == 1
    assert r.uncertainty_resolve_pages_max == 2


def test_build_report_uncertainty_unresolved_from_anomalies() -> None:
    recovery = PaperSessionRecoveryReport()
    recovery.rehydration_anomalies.append("SUBMIT_UNCERTAINTY_UNRESOLVED: order_id=z")
    st = PortfolioState.initial(Decimal("100"))
    replay = replay_portfolio_state([], Decimal("100"))
    r = build_paper_session_operator_report(
        session_id=SessionId("00000000-0000-4000-8000-00000000a002"),
        adapter_mode="alpaca",
        resumed_session=False,
        bars_processed=0,
        events=[],
        state=st,
        recovery=recovery,
        replay=replay,
    )
    assert r.uncertainty_unresolved is True


def test_build_report_pagination_totals() -> None:
    recovery = PaperSessionRecoveryReport()
    recovery.cumulative_pagination_order_pages = 3
    recovery.cumulative_pagination_activity_pages = 2
    st = PortfolioState.initial(Decimal("100"))
    replay = replay_portfolio_state([], Decimal("100"))
    r = build_paper_session_operator_report(
        session_id=SessionId("00000000-0000-4000-8000-00000000a003"),
        adapter_mode="alpaca",
        resumed_session=False,
        bars_processed=0,
        events=[],
        state=st,
        recovery=recovery,
        replay=replay,
    )
    assert r.pages_polled == 5
    assert r.pages_polled_orders == 3
    assert r.pages_polled_activities == 2


def test_run_paper_session_dry_run_adapter_mode(tmp_path) -> None:
    db = str(tmp_path / "d.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000d001")
    out = run_paper_session(
        api=api,
        store=store,
        session_id=sid,
        strategy=SampleBarCrossStrategy(trade_qty=Decimal("10")),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_risk(),
        broker=MockBrokerAdapter(),
        dry_run=True,
    )
    conn.close()
    assert out.report.adapter_mode == "dry_run"
    assert out.report.recovery_ran is True
    assert out.report.bars_processed >= 1


def test_run_paper_session_mock_adapter_mode(tmp_path) -> None:
    db = str(tmp_path / "m.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000d002")
    out = run_paper_session(
        api=api,
        store=store,
        session_id=sid,
        strategy=SampleBarCrossStrategy(trade_qty=Decimal("10")),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_risk(),
        broker=MockBrokerAdapter(),
        dry_run=False,
    )
    conn.close()
    assert out.report.adapter_mode == "mock"


def test_run_paper_session_resumed_flag(tmp_path) -> None:
    db = str(tmp_path / "r.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000d003")
    sm = SessionManager(store, sid)
    sm.append(session_started(sid, "paper", "2024-01-01T00:00:00Z"))
    out = run_paper_session(
        api=api,
        store=store,
        session_id=sid,
        strategy=SampleBarCrossStrategy(trade_qty=Decimal("10")),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_risk(),
        broker=MockBrokerAdapter(),
        resume_existing_session=True,
    )
    conn.close()
    assert out.report.resumed_session is True
    assert out.report.started_fresh is False


def test_startup_broker_facts_visible_in_report(tmp_path) -> None:
    iid = InstrumentId("00000000-0000-4000-8000-000000000001")
    db = str(tmp_path / "su.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    broker = MockBrokerAdapter(synthetic_fill_enabled=False)
    broker.inject_startup_fact(
        BrokerFillFact(
            client_order_id=OrderId("00000000-0000-4000-8000-00000000e099"),
            instrument_id=iid,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            price=Decimal("50"),
            fees=Decimal("0"),
            fill_ts_utc="2024-01-01T00:00:00Z",
            occurred_at_utc="2024-01-01T00:00:00Z",
            broker_execution_id="startup-exec-1",
        )
    )
    sid = SessionId("00000000-0000-4000-8000-00000000e100")
    out = run_paper_session(
        api=api,
        store=store,
        session_id=sid,
        strategy=SampleBarCrossStrategy(trade_qty=Decimal("10")),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_risk(),
        broker=broker,
    )
    evs = store.load_session(str(sid))
    conn.close()
    assert out.report.startup_broker_facts_seen >= 1
    assert any(e.event_type == EventType.FILL_RECEIVED for e in evs)


def test_cli_paper_dry_run_json_no_network(tmp_path) -> None:
    db = str(tmp_path / "cli.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    conn.close()
    runner = CliRunner()
    r = runner.invoke(app, ["paper-dry-run", "--db-path", db, "--json", "--session-id", "00000000-0000-4000-8000-00000000f001"])
    assert r.exit_code == 0, r.stdout
    assert "dry_run" in r.stdout
    assert "recovery_ran" in r.stdout
    assert "pages_polled" in r.stdout


def test_cli_paper_quiet_line(tmp_path) -> None:
    db = str(tmp_path / "q.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    conn.close()
    runner = CliRunner()
    r = runner.invoke(
        app,
        [
            "paper",
            "--db-path",
            db,
            "--quiet",
            "--adapter",
            "mock",
            "--session-id",
            "00000000-0000-4000-8000-00000000f002",
        ],
    )
    assert r.exit_code == 0
    assert "Paper session" in r.stdout
    assert "Submits=" in r.stdout
