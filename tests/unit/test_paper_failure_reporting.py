"""Partial failure operator reporting for paper sessions (deterministic, no network)."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

from typer.testing import CliRunner

from gkr_trading.cli import seed
from gkr_trading.cli.main import app
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import Timeframe
from gkr_trading.core.schemas.ids import SessionId
from gkr_trading.data.access_api.service import DataAccessAPI
from gkr_trading.live.alpaca_config import AlpacaPaperConfig
from gkr_trading.live.alpaca_paper_mapping import AlpacaMalformedPayloadError
from gkr_trading.live.alpaca_paper_adapter import AlpacaPaperAdapter
from gkr_trading.live.paper_session_report import PaperSessionFailureReport, PaperSessionRunFailed
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


class FailStartupBroker:
    """Broker that fails during STARTUP poll (simulated adapter outage)."""

    def submit(self, req):  # pragma: no cover
        raise RuntimeError("submit should not be called")

    def poll_broker_facts(self, *, cursor, hints, phase):
        if str(phase) == "startup":
            raise RuntimeError("simulated startup poll failure")
        return __import__("gkr_trading.live.broker_adapter", fromlist=["BrokerPollResult"]).BrokerPollResult(
            facts=tuple(),
            cursor=__import__(
                "gkr_trading.live.broker_adapter", fromlist=["BrokerReconciliationCursor"]
            ).BrokerReconciliationCursor(token="x"),
        )


def test_failure_during_startup_recovery_emits_partial_report(tmp_path) -> None:
    db = str(tmp_path / "s.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000fa01")
    try:
        run_paper_session(
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
            broker=FailStartupBroker(),
        )
        raise AssertionError("expected failure")
    except PaperSessionRunFailed as e:
        rep = e.report
        assert rep.status == "failed"
        assert rep.session_id == str(sid)
        assert rep.startup_recovery_ran is True
        assert rep.startup_recovery_completed is False
        assert rep.failure_phase == "startup_sync"
        assert rep.bars_processed in (None, 0)
    finally:
        conn.close()


class MalformedHttp:
    """Return one malformed fill activity: missing id field."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self._last_client_order_id: str | None = None
        self._alpaca_order_id = "alp-1"

    def request_json(self, method: str, path: str, *, query=None, json_body=None):
        self.calls.append((method, path))
        if method == "POST" and path == "/v2/orders":
            # Successful submit; capture client_order_id so /v2/orders can include it.
            self._last_client_order_id = (json_body or {}).get("client_order_id")
            return {"id": self._alpaca_order_id, "status": "accepted"}
        if method == "GET" and path == "/v2/orders":
            cid = self._last_client_order_id or "unknown"
            return [{"id": self._alpaca_order_id, "client_order_id": cid, "status": "accepted"}]
        if method == "GET" and "/account/activities" in path:
            return [
                {
                    "activity_type": "FILL",
                    "id": "bad-activity-1",
                    "order_id": self._alpaca_order_id,
                    # Missing qty => AlpacaMalformedPayloadError
                    "price": "1",
                    "side": "buy",
                    "transaction_time": "2024-01-01T00:00:00Z",
                }
            ]
        return []


def test_malformed_broker_payload_during_poll_emits_partial_report(tmp_path) -> None:
    db = str(tmp_path / "m.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000fa02")
    cfg = AlpacaPaperConfig(api_key="k", secret_key="s")
    http = MalformedHttp()
    brk = AlpacaPaperAdapter(cfg, http=http)
    try:
        run_paper_session(
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
            broker=brk,
            symbol_resolver=lambda _iid: "DEMO",
        )
        raise AssertionError("expected failure")
    except PaperSessionRunFailed as e:
        rep = e.report
        assert rep.status == "failed"
        assert rep.adapter_mode == "alpaca"
        assert rep.failure_type == AlpacaMalformedPayloadError.__name__
        assert rep.failure_phase is not None
        assert rep.pages_polled is not None or rep.pages_polled_orders is not None
    finally:
        conn.close()


def test_cli_paper_json_failure_exits_nonzero(tmp_path, monkeypatch) -> None:
    # Force failure by patching the CLI's MockBrokerAdapter used in paper.
    from gkr_trading.cli import main as cli_main

    class FailingMock(cli_main.MockBrokerAdapter):
        def poll_broker_facts(self, *, cursor, hints, phase):
            raise RuntimeError("simulated broker poll failure")

    monkeypatch.setattr(cli_main, "MockBrokerAdapter", FailingMock)

    db = str(tmp_path / "cli.db")
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
            "--json",
            "--adapter",
            "mock",
            "--session-id",
            "00000000-0000-4000-8000-00000000fb01",
        ],
    )
    assert r.exit_code != 0
    assert "\"status\": \"failed\"" in r.stdout
    assert "\"failure_type\"" in r.stdout


def test_cli_paper_quiet_failure_exits_nonzero(tmp_path, monkeypatch) -> None:
    from gkr_trading.cli import main as cli_main

    class FailingMock(cli_main.MockBrokerAdapter):
        def poll_broker_facts(self, *, cursor, hints, phase):
            raise RuntimeError("simulated broker poll failure")

    monkeypatch.setattr(cli_main, "MockBrokerAdapter", FailingMock)

    db = str(tmp_path / "cli2.db")
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
            "00000000-0000-4000-8000-00000000fb02",
        ],
    )
    assert r.exit_code != 0
    assert "FAILED" in r.stdout

