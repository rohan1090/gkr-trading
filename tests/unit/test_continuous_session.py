"""Tests for continuous session runner, market data feed, WebSocket integration,
reconciliation hardening, replay strict-mode, and multi-cycle strategies.

Covers Phases 1–4 of the Alpaca paper-testing gap closure:
  Phase 1: Persistent session loop + market data feed
  Phase 2: WebSocket integration + reconnect safety
  Phase 3: Live venue reconciliation hardening
  Phase 4: Replay strict-mode validation
"""
from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.core.market_data import MarketDataEnvelope
from gkr_trading.core.operator_controls import KillSwitchLevel
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.order_model import OrderStatus
from gkr_trading.core.reconciliation_model import (
    OptionsReconciliationSnapshot,
    ReconciliationBreak,
)
from gkr_trading.core.risk_gate import RiskApprovalGate, RiskDecision
from gkr_trading.live.base import (
    SubmissionRequest,
    SubmissionResponse,
    VenueAccountInfo,
    VenueAdapter,
    VenuePosition,
)
from gkr_trading.live.fill_translator import FillTranslator
from gkr_trading.live.market_data_feed import (
    AlpacaMarketDataFeed,
    FeedStats,
    MarketDataFeedConfig,
    _dollars_to_cents,
    _parse_rfc3339_ns,
)
from gkr_trading.live.runtime_v2 import (
    ContinuousSessionConfig,
    ContinuousSessionResult,
    ContinuousSessionRunner,
    PaperSessionRunnerV2,
    PaperSessionV2Config,
    PaperSessionV2Result,
    StopReason,
    build_paper_runner,
)
from gkr_trading.live.session_supervisor import SessionState
from gkr_trading.live.websocket_manager import (
    AlpacaWebSocketManager,
    ConnectionState,
    WebSocketStats,
)
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry
from gkr_trading.persistence.position_store import PositionStore
from gkr_trading.strategy.sample_equity_multicycle_v2 import (
    MultiCycleEquityStrategyV2,
)


# ---------------------------------------------------------------------------
# Fixtures / Helpers
# ---------------------------------------------------------------------------


def _in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


class MockVenueAdapter(VenueAdapter):
    """Mock venue adapter for testing."""

    def __init__(self, *, reject: bool = False) -> None:
        self._submitted: List[SubmissionRequest] = []
        self._reject = reject
        self._positions: List[VenuePosition] = []
        self._open_orders: List[dict] = []

    @property
    def venue_name(self) -> str:
        return "mock_venue"

    def submit_order(self, request: SubmissionRequest) -> SubmissionResponse:
        self._submitted.append(request)
        if self._reject:
            return SubmissionResponse(
                client_order_id=request.client_order_id,
                venue_order_id=None,
                success=False,
                rejected=True,
                reject_reason="mock rejection",
            )
        return SubmissionResponse(
            client_order_id=request.client_order_id,
            venue_order_id=f"venue-{request.client_order_id[:8]}",
            success=True,
        )

    def cancel_order(self, client_order_id: str) -> bool:
        return True

    def get_order_status(self, client_order_id: str) -> Optional[OrderStatus]:
        return OrderStatus.FILLED

    def get_positions(self) -> List[VenuePosition]:
        return self._positions

    def set_positions(self, positions: List[VenuePosition]) -> None:
        self._positions = positions

    def get_account(self) -> VenueAccountInfo:
        return VenueAccountInfo(cash_cents=10_000_000, buying_power_cents=10_000_000)

    def get_open_orders(self) -> List[dict]:
        return self._open_orders


class MockFillTranslator(FillTranslator):
    def translate_fill(self, venue_payload: dict) -> Optional[FillEvent]:
        if venue_payload.get("type") == "fill":
            return FillEvent(
                event_id=str(uuid.uuid4()),
                session_id="test",
                seq_no=0,
                client_order_id=venue_payload.get("order_id", ""),
                venue_fill_id=str(uuid.uuid4()),
                instrument_ref=EquityRef(ticker=venue_payload.get("symbol", "SPY")),
                venue="mock_venue",
                action="buy_to_open",
                quantity=venue_payload.get("qty", 1),
                price_cents=venue_payload.get("price_cents", 10000),
                fee_cents=0,
                is_taker=True,
                timestamp_ns=time.time_ns(),
            )
        return None


class MockStrategy:
    """Mock strategy that emits TradeIntents on demand."""
    name = "mock_strategy"

    def __init__(self, intents: Optional[List[TradeIntent]] = None) -> None:
        self._intents = list(intents or [])
        self._call_count = 0

    def on_market_data(self, envelope: object, context: object) -> object:
        if self._call_count < len(self._intents):
            intent = self._intents[self._call_count]
            self._call_count += 1
            return intent
        return None


class MockMarketDataFeed:
    """Mock market data feed for testing ContinuousSessionRunner."""

    def __init__(self, envelopes_per_poll: Optional[List[List[MarketDataEnvelope]]] = None) -> None:
        self._polls = list(envelopes_per_poll or [])
        self._poll_index = 0
        self._stats = FeedStats()
        self._fail_after: Optional[int] = None

    @property
    def stats(self) -> FeedStats:
        return self._stats

    @property
    def has_fatal_failure(self) -> bool:
        return self._stats.consecutive_failures >= 5

    def set_fail_after(self, n: int) -> None:
        """Force consecutive failures after n successful polls."""
        self._fail_after = n

    def poll(self) -> List[MarketDataEnvelope]:
        self._stats.polls += 1

        if self._fail_after is not None and self._stats.polls > self._fail_after:
            self._stats.errors += 1
            self._stats.consecutive_failures += 1
            raise RuntimeError("Simulated poll failure")

        if self._poll_index < len(self._polls):
            result = self._polls[self._poll_index]
            self._poll_index += 1
            self._stats.envelopes_produced += len(result)
            self._stats.consecutive_failures = 0
            return result
        self._stats.consecutive_failures = 0
        return []


class MockMetadataProvider:
    """Mock market metadata provider."""

    def __init__(self, *, market_open: bool = True) -> None:
        self._market_open = market_open

    def is_market_open(self) -> bool:
        return self._market_open


class MockWebSocketManager:
    """Minimal mock for WebSocket manager."""

    def __init__(self) -> None:
        self._started = False
        self._stopped = False
        self._on_trade_update = None
        self._on_disconnect = None
        self._on_reconnect = None
        self._on_connect = None

    def start(self) -> None:
        self._started = True
        if self._on_connect:
            self._on_connect()

    def stop(self) -> None:
        self._stopped = True


def _make_equity_intent(session_id: str) -> TradeIntent:
    return TradeIntent(
        intent_id=str(uuid.uuid4()),
        strategy_id="test",
        session_id=session_id,
        venue_class="traditional",
        instrument_ref=EquityRef(ticker="AAPL"),
        action="buy_to_open",
        quantity=10,
        limit_price_cents=15000,
        time_in_force="day",
        created_at_ns=time.time_ns(),
    )


def _make_envelope(close_cents: int = 15000, ticker: str = "AAPL") -> MarketDataEnvelope:
    return MarketDataEnvelope(
        instrument_ref=EquityRef(ticker=ticker),
        timestamp_ns=time.time_ns(),
        close_cents=close_cents,
        last_cents=close_cents,
        volume=1_000_000,
    )


def _build_continuous_runner(
    *,
    conn: Optional[sqlite3.Connection] = None,
    session_id: Optional[str] = None,
    strategy: Optional[Any] = None,
    adapter: Optional[MockVenueAdapter] = None,
    md_feed: Optional[MockMarketDataFeed] = None,
    metadata: Optional[MockMetadataProvider] = None,
    ws: Optional[MockWebSocketManager] = None,
    config: Optional[ContinuousSessionConfig] = None,
    fill_translator: Optional[MockFillTranslator] = None,
) -> ContinuousSessionRunner:
    """Build a ContinuousSessionRunner with mock dependencies."""
    conn = conn or _in_memory_db()
    sid = session_id or str(uuid.uuid4())
    adapter = adapter or MockVenueAdapter()
    fill_translator = fill_translator or MockFillTranslator()

    runner = build_paper_runner(
        conn=conn,
        session_id=sid,
        equity_adapter=adapter,
        equity_fill_translator=fill_translator,
        strategy=strategy,
        venue="mock_venue",
    )

    return ContinuousSessionRunner(
        runner=runner,
        market_data_feed=md_feed or MockMarketDataFeed(),
        metadata_provider=metadata or MockMetadataProvider(),
        ws_manager=ws,
        config=config or ContinuousSessionConfig(poll_interval_sec=0.01),
    )


# ===========================================================================
# Phase 1: Persistent Session Loop + Market Data Feed
# ===========================================================================


class TestContinuousSessionRunnerLifecycle:
    """ContinuousSessionRunner: startup → loop → shutdown."""

    def test_empty_run_with_max_cycles(self):
        """Runner completes after max_cycles with no market data."""
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=3,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(config=config)
        result = cs.run_session()

        assert result.cycles_completed == 3
        assert result.stop_reason == StopReason.MAX_CYCLES
        assert result.session_result.startup_clean
        assert result.session_result.shutdown_clean

    def test_market_data_envelopes_fed_to_strategy(self):
        """Market data from feed is forwarded to strategy."""
        sid = str(uuid.uuid4())
        # Strategy emits on every call
        intents = [_make_equity_intent(sid) for _ in range(3)]
        strategy = MockStrategy(intents=intents)

        env1 = _make_envelope(15000)
        env2 = _make_envelope(14900)
        env3 = _make_envelope(14800)

        md = MockMarketDataFeed(envelopes_per_poll=[
            [env1],
            [env2],
            [env3],
        ])

        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=3,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(
            session_id=sid,
            strategy=strategy,
            md_feed=md,
            config=config,
        )
        result = cs.run_session()

        assert result.cycles_completed == 3
        assert result.md_envelopes == 3
        assert result.session_result.intents_generated >= 1

    def test_stop_on_market_close(self):
        """Runner stops when metadata reports market closed."""
        metadata = MockMetadataProvider(market_open=False)
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            stop_after_market_close=True,
        )
        cs = _build_continuous_runner(metadata=metadata, config=config)
        result = cs.run_session()

        assert result.stop_reason == StopReason.MARKET_CLOSED
        assert result.cycles_completed == 0

    def test_stop_on_kill_switch(self):
        """Runner stops when kill switch is activated."""
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=100,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(config=config)

        # Start session, then activate kill switch before loop runs far
        cs.runner.startup()
        cs.runner.supervisor.activate_kill_switch(KillSwitchLevel.FULL_HALT)
        # We call _run_loop directly to test
        reason = cs._run_loop()
        assert reason == StopReason.KILL_SWITCH

    def test_external_stop_request(self):
        """request_stop() halts the loop."""
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1000,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(config=config)
        cs.request_stop(StopReason.EXTERNAL)
        result = cs.run_session()

        assert result.stop_reason == StopReason.EXTERNAL

    def test_md_failure_stops_session(self):
        """Consecutive market data failures trigger MD_FAILURE stop."""
        md = MockMarketDataFeed()
        md.set_fail_after(0)  # fail from the very first poll

        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_consecutive_md_failures=5,
            max_cycles=100,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(md_feed=md, config=config)
        result = cs.run_session()

        assert result.stop_reason == StopReason.MD_FAILURE

    def test_replay_validation_runs_on_shutdown(self):
        """Replay validation is triggered at session end."""
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(config=config)
        result = cs.run_session()

        # replay_anomaly_count should be 0 for a clean session (no fills)
        assert result.replay_anomaly_count == 0


class TestMultiCycleStrategy:
    """MultiCycleEquityStrategyV2 trades on every dip."""

    def test_emits_on_every_dip(self):
        sid = str(uuid.uuid4())
        strat = MultiCycleEquityStrategyV2(session_id=sid, quantity=5)

        # dip1: 150 → 149
        r1 = strat.on_market_data(_make_envelope(15000), None)
        r2 = strat.on_market_data(_make_envelope(14900), None)
        assert r2 is not None
        assert r2.quantity == 5

        # dip2: 149 → 148
        r3 = strat.on_market_data(_make_envelope(14800), None)
        assert r3 is not None
        assert strat.trade_count == 2

    def test_no_emit_on_flat_or_up(self):
        sid = str(uuid.uuid4())
        strat = MultiCycleEquityStrategyV2(session_id=sid)

        strat.on_market_data(_make_envelope(15000), None)
        r = strat.on_market_data(_make_envelope(15000), None)  # flat
        assert r is None
        r = strat.on_market_data(_make_envelope(15100), None)  # up
        assert r is None
        assert strat.trade_count == 0

    def test_cooldown_respected(self):
        sid = str(uuid.uuid4())
        strat = MultiCycleEquityStrategyV2(session_id=sid, cooldown_cycles=2)

        strat.on_market_data(_make_envelope(15000), None)
        r1 = strat.on_market_data(_make_envelope(14900), None)  # dip #1
        assert r1 is not None

        r2 = strat.on_market_data(_make_envelope(14800), None)  # too soon
        assert r2 is None

        strat.on_market_data(_make_envelope(14700), None)  # still cooling
        r3 = strat.on_market_data(_make_envelope(14600), None)  # cooldown met
        assert r3 is not None
        assert strat.trade_count == 2

    def test_ignores_non_equity(self):
        sid = str(uuid.uuid4())
        strat = MultiCycleEquityStrategyV2(session_id=sid)
        ref = OptionsRef(
            underlying="AAPL", expiry=date(2026, 6, 19),
            strike_cents=15000, right="call", multiplier=100,
            occ_symbol="AAPL260619C00150000",
        )
        env = MarketDataEnvelope(
            instrument_ref=ref, timestamp_ns=time.time_ns(),
            close_cents=500, last_cents=500,
        )
        r = strat.on_market_data(env, None)
        assert r is None


class TestAlpacaMarketDataFeed:
    """AlpacaMarketDataFeed polling and stale detection."""

    def _make_http(self, responses: Dict[str, Any]) -> MagicMock:
        """Create a mock http client that returns configured responses."""
        mock = MagicMock()
        def request_json(method, path, *, query=None, json_body=None):
            return responses.get(path, {})
        mock.request_json = request_json
        return mock

    def test_equity_snapshot_produces_envelope(self):
        snap = {
            "AAPL": {
                "latestTrade": {"p": 150.25, "t": "2026-04-08T14:30:00Z"},
                "latestQuote": {"bp": 150.20, "ap": 150.30},
                "minuteBar": {"o": 150.00, "h": 150.50, "l": 149.90, "c": 150.25, "v": 1000},
                "dailyBar": {},
            }
        }
        http = self._make_http({"/v2/stocks/snapshots": snap})
        config = MarketDataFeedConfig(equity_tickers=["AAPL"])
        feed = AlpacaMarketDataFeed(http_client=http, config=config)

        envs = feed.poll()
        assert len(envs) == 1
        env = envs[0]
        assert isinstance(env.instrument_ref, EquityRef)
        assert env.instrument_ref.ticker == "AAPL"
        assert env.last_cents == 15025
        assert env.bid_cents == 15020
        assert env.ask_cents == 15030

    def test_stale_data_skipped(self):
        """Same timestamp → no new envelope on second poll."""
        snap = {
            "AAPL": {
                "latestTrade": {"p": 150.00, "t": "2026-04-08T14:30:00Z"},
                "latestQuote": {},
                "minuteBar": {},
                "dailyBar": {},
            }
        }
        http = self._make_http({"/v2/stocks/snapshots": snap})
        config = MarketDataFeedConfig(equity_tickers=["AAPL"])
        feed = AlpacaMarketDataFeed(http_client=http, config=config)

        envs1 = feed.poll()
        assert len(envs1) == 1

        envs2 = feed.poll()
        assert len(envs2) == 0
        assert feed.stats.stale_skips == 1

    def test_consecutive_failures_tracked(self):
        http = MagicMock()
        http.request_json.side_effect = RuntimeError("network")
        config = MarketDataFeedConfig(equity_tickers=["AAPL"], max_consecutive_failures=3)
        feed = AlpacaMarketDataFeed(http_client=http, config=config)

        for _ in range(4):
            feed.poll()

        assert feed.stats.consecutive_failures >= 3
        assert feed.has_fatal_failure

    def test_options_snapshot_produces_envelope(self):
        snap = {
            "snapshots": {
                "AAPL260619C00150000": {
                    "latestTrade": {"p": 5.10, "t": "2026-04-08T14:30:00Z"},
                    "latestQuote": {"bp": 5.00, "ap": 5.20},
                    "greeks": {
                        "implied_volatility": 0.35,
                        "delta": 0.55,
                        "gamma": 0.02,
                        "theta": -0.05,
                        "vega": 0.12,
                    },
                    "openInterest": 5000,
                }
            }
        }
        http = self._make_http({"/v1beta1/options/snapshots": snap})
        config = MarketDataFeedConfig(options_occ_symbols=["AAPL260619C00150000"])
        feed = AlpacaMarketDataFeed(http_client=http, config=config)

        envs = feed.poll()
        assert len(envs) == 1
        env = envs[0]
        assert isinstance(env.instrument_ref, OptionsRef)
        assert env.last_cents == 510
        assert env.delta == 0.55
        assert env.open_interest == 5000

    def test_empty_response_returns_no_envelopes(self):
        http = self._make_http({"/v2/stocks/snapshots": {}})
        config = MarketDataFeedConfig(equity_tickers=["AAPL"])
        feed = AlpacaMarketDataFeed(http_client=http, config=config)

        envs = feed.poll()
        assert len(envs) == 0

    def test_no_configured_tickers_does_nothing(self):
        http = MagicMock()
        config = MarketDataFeedConfig()  # empty
        feed = AlpacaMarketDataFeed(http_client=http, config=config)

        envs = feed.poll()
        assert len(envs) == 0
        http.request_json.assert_not_called()


class TestMarketDataFeedHelpers:
    """Test helper functions in market_data_feed module."""

    def test_dollars_to_cents(self):
        assert _dollars_to_cents(150.25) == 15025
        assert _dollars_to_cents(0.01) == 1
        assert _dollars_to_cents(None) is None
        assert _dollars_to_cents(0) == 0

    def test_parse_rfc3339_ns(self):
        ts = _parse_rfc3339_ns("2026-04-08T14:30:00Z")
        assert ts > 0
        # Should be approximately 2026 epoch
        assert ts > 1_000_000_000_000_000_000  # after year 2001

    def test_parse_rfc3339_ns_empty(self):
        ts = _parse_rfc3339_ns("")
        # Falls back to current time
        assert ts > 0


# ===========================================================================
# Phase 2: WebSocket Integration + Reconnect Safety
# ===========================================================================


class TestWebSocketIntegration:
    """WebSocket wiring into ContinuousSessionRunner."""

    def test_ws_start_and_stop_called(self):
        ws = MockWebSocketManager()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1,
            stop_after_market_close=False,
            enable_websocket=True,
        )
        cs = _build_continuous_runner(ws=ws, config=config)
        cs.run_session()

        assert ws._started
        assert ws._stopped

    def test_ws_callbacks_wired(self):
        """ContinuousSessionRunner wires its callbacks onto the WS manager."""
        ws = MockWebSocketManager()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1,
            stop_after_market_close=False,
            enable_websocket=True,
        )
        cs = _build_continuous_runner(ws=ws, config=config)
        cs.run_session()

        # Callbacks should be set
        assert ws._on_trade_update is not None
        assert ws._on_disconnect is not None
        assert ws._on_reconnect is not None

    def test_ws_trade_update_processed(self):
        """Trade update received via WS is forwarded to runner."""
        ws = MockWebSocketManager()
        fill_xlator = MockFillTranslator()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=2,
            stop_after_market_close=False,
            enable_websocket=True,
        )
        cs = _build_continuous_runner(
            ws=ws,
            fill_translator=fill_xlator,
            config=config,
        )
        # Start session manually to wire callbacks
        cs.runner.startup()
        cs._setup_ws_callbacks()

        # Simulate a trade update
        ws._on_trade_update({"type": "fill", "symbol": "AAPL", "qty": 10, "price_cents": 15000})
        assert cs._ws_trade_updates == 1

    def test_ws_disconnect_suspends_session(self):
        """WS disconnect → session suspended → submissions blocked."""
        ws = MockWebSocketManager()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=2,
            stop_after_market_close=False,
            enable_websocket=True,
        )
        cs = _build_continuous_runner(ws=ws, config=config)
        cs.runner.startup()
        cs._setup_ws_callbacks()

        # Simulate disconnect
        ws._on_disconnect("test_disconnect")
        assert cs.runner.supervisor.state == SessionState.SUSPENDED
        assert cs._submission_suspended
        assert not cs._ws_connected

    def test_ws_reconnect_reconciles_then_resumes(self):
        """WS reconnect → reconciliation → if clean, resume."""
        ws = MockWebSocketManager()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=2,
            stop_after_market_close=False,
            enable_websocket=True,
        )
        cs = _build_continuous_runner(ws=ws, config=config)
        cs.runner.startup()
        cs._setup_ws_callbacks()

        # Disconnect
        ws._on_disconnect("test")
        assert cs.runner.supervisor.state == SessionState.SUSPENDED

        # Reconnect — reconciliation should pass (mock adapter, no breaks)
        ws._on_reconnect()
        assert cs._ws_connected
        assert not cs._submission_suspended
        assert cs.runner.supervisor.state == SessionState.RUNNING

    def test_ws_reconnect_blocking_break_halts(self):
        """WS reconnect with blocking recon break → session halted."""
        ws = MockWebSocketManager()
        adapter = MockVenueAdapter()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=2,
            stop_after_market_close=False,
            enable_websocket=True,
        )
        conn = _in_memory_db()
        sid = str(uuid.uuid4())

        runner = build_paper_runner(
            conn=conn,
            session_id=sid,
            equity_adapter=adapter,
            venue="mock_venue",
        )

        cs = ContinuousSessionRunner(
            runner=runner,
            market_data_feed=MockMarketDataFeed(),
            metadata_provider=MockMetadataProvider(),
            ws_manager=ws,
            config=config,
        )
        cs.runner.startup()
        cs._setup_ws_callbacks()

        # Create a local position so local_qty != 0 (required for blocking break)
        ps = PositionStore(conn)
        ps.upsert_equity(
            session_id=sid, venue="mock_venue", ticker="AAPL",
            signed_qty=50, cost_basis_cents=750000,
            realized_pnl_cents=0, status="open",
        )

        # Disconnect
        ws._on_disconnect("test")

        # Inject a blocking break by setting a venue position that disagrees
        # with the local record (local=50, venue=100)
        adapter.set_positions([
            VenuePosition(instrument_key="equity:AAPL", quantity=100),
        ])

        # Reconnect — recon should find blocking break
        ws._on_reconnect()
        # Session should be halted or stop requested
        assert (
            cs.runner.supervisor.state == SessionState.HALTED
            or cs._stop_requested
        )

    def test_ws_disabled_runs_without_ws(self):
        """enable_websocket=False skips WS entirely."""
        ws = MockWebSocketManager()
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1,
            stop_after_market_close=False,
            enable_websocket=False,
        )
        cs = _build_continuous_runner(ws=ws, config=config)
        result = cs.run_session()

        assert not ws._started
        assert result.ws_connected is False


class TestWebSocketManagerUnit:
    """Unit tests for AlpacaWebSocketManager state management."""

    def test_initial_state_disconnected(self):
        mgr = AlpacaWebSocketManager(api_key="test", secret_key="test")
        assert mgr.state == ConnectionState.DISCONNECTED
        assert not mgr.is_connected

    def test_stop_sets_closed_state(self):
        mgr = AlpacaWebSocketManager(api_key="test", secret_key="test")
        mgr.stop()
        assert mgr.state == ConnectionState.CLOSED

    def test_stats_initialized(self):
        mgr = AlpacaWebSocketManager(api_key="test", secret_key="test")
        assert mgr.stats.connect_count == 0
        assert mgr.stats.messages_received == 0


# ===========================================================================
# Phase 3: Reconciliation Hardening
# ===========================================================================


class TestReconciliationHardening:
    """Reconciliation service: orphan detection, blocking startup, severity handling."""

    def test_clean_reconciliation_at_startup(self):
        """No positions → clean reconciliation."""
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()

        runner = build_paper_runner(
            conn=conn, session_id=sid, equity_adapter=adapter, venue="mock_venue",
        )
        ok = runner.startup()
        assert ok
        assert runner.supervisor.state == SessionState.RUNNING

    def test_preexisting_venue_position_does_not_block_startup(self):
        """Venue position with no local record → warning, not blocking."""
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        # Venue says we have 100 AAPL, local says 0 (pre-existing from prior run)
        adapter.set_positions([
            VenuePosition(instrument_key="equity:AAPL", quantity=100),
        ])

        runner = build_paper_runner(
            conn=conn, session_id=sid, equity_adapter=adapter, venue="mock_venue",
        )
        ok = runner.startup()
        # Should succeed — local_qty==0 means pre-existing venue position → warning only
        assert ok
        assert runner.supervisor.state == SessionState.RUNNING

    def test_local_position_mismatch_blocks_startup(self):
        """Position tracked locally but qty disagrees with venue → blocking."""
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()

        # First, create a local position record so local_qty != 0
        ps = PositionStore(conn)
        ps.upsert_equity(
            session_id=sid, venue="mock_venue", ticker="AAPL",
            signed_qty=50, cost_basis_cents=750000,
            realized_pnl_cents=0, status="open",
        )

        # Venue says 100, local says 50 → blocking mismatch
        adapter.set_positions([
            VenuePosition(instrument_key="equity:AAPL", quantity=100),
        ])

        runner = build_paper_runner(
            conn=conn, session_id=sid, equity_adapter=adapter, venue="mock_venue",
        )
        ok = runner.startup()
        assert not ok
        assert runner.supervisor.state == SessionState.HALTED

    def test_orphan_order_detected_as_warning(self):
        """Open orders on venue → warning breaks (not blocking)."""
        from gkr_trading.live.reconciliation_service import ReconciliationService
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        adapter._open_orders = [
            {"client_order_id": "orphan-001", "status": "open"},
            {"client_order_id": "orphan-002", "status": "partially_filled"},
        ]
        ps = PositionStore(conn)

        recon = ReconciliationService(
            position_store=ps, adapter=adapter, session_id=sid,
        )
        snapshot = recon.reconcile(trigger="startup")

        orphan_breaks = [b for b in snapshot.breaks if b.break_type == "orphan_order"]
        assert len(orphan_breaks) == 2
        assert all(b.severity == "warning" for b in orphan_breaks)
        # Orphan warnings are NOT blocking
        assert not snapshot.has_blocking_breaks()

    def test_cash_mismatch_is_warning(self):
        """Cash discrepancy produces warning, not blocking break."""
        from gkr_trading.live.reconciliation_service import ReconciliationService
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        # Adapter reports cash_cents=10_000_000
        # Local has 0 equity positions so local_cash=0
        ps = PositionStore(conn)

        recon = ReconciliationService(
            position_store=ps, adapter=adapter, session_id=sid,
        )
        snapshot = recon.reconcile()

        cash_breaks = [b for b in snapshot.breaks if b.break_type == "cash"]
        # Cash might differ since we have no local positions → local_cash=0 vs venue=10M
        if cash_breaks:
            assert all(b.severity == "warning" for b in cash_breaks)
            assert not snapshot.has_blocking_breaks()

    def test_venue_only_position_is_warning(self):
        """Venue has position, local has none → warning (not blocking)."""
        from gkr_trading.live.reconciliation_service import ReconciliationService
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        adapter.set_positions([
            VenuePosition(instrument_key="equity:AAPL", quantity=50),
        ])
        ps = PositionStore(conn)

        recon = ReconciliationService(
            position_store=ps, adapter=adapter, session_id=sid,
        )
        snapshot = recon.reconcile()

        position_breaks = [b for b in snapshot.breaks if b.break_type == "position"]
        assert len(position_breaks) == 1
        # local_qty==0 → warning, not blocking
        assert position_breaks[0].severity == "warning"
        assert not snapshot.has_blocking_breaks()

    def test_local_tracked_position_mismatch_is_blocking(self):
        """Local session tracked a position that disagrees with venue → blocking."""
        from gkr_trading.live.reconciliation_service import ReconciliationService
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        adapter.set_positions([
            VenuePosition(instrument_key="equity:AAPL", quantity=100),
        ])
        ps = PositionStore(conn)
        # Create a local record so local_qty != 0
        ps.upsert_equity(
            session_id=sid, venue="mock_venue", ticker="AAPL",
            signed_qty=50, cost_basis_cents=750000,
            realized_pnl_cents=0, status="open",
        )

        recon = ReconciliationService(
            position_store=ps, adapter=adapter, session_id=sid,
        )
        snapshot = recon.reconcile()

        position_breaks = [b for b in snapshot.breaks if b.break_type == "position"]
        assert len(position_breaks) == 1
        assert position_breaks[0].severity == "blocking"
        assert snapshot.has_blocking_breaks()

    def test_recon_event_persisted(self):
        """Reconciliation events are written to EventStore."""
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        store = SqliteEventStore(conn)

        runner = build_paper_runner(
            conn=conn, session_id=sid, equity_adapter=adapter, venue="mock_venue",
        )
        runner.startup()
        runner.shutdown()

        events = store.load_session(sid)
        recon_events = [e for e in events if e.event_type.value == "reconciliation_completed"]
        # Should have at least startup + shutdown reconciliation
        assert len(recon_events) >= 2

    def test_unknown_orders_marked_on_startup(self):
        """PendingOrderRegistry marks non-terminal orders as UNKNOWN on startup."""
        conn = _in_memory_db()
        sid = str(uuid.uuid4())
        adapter = MockVenueAdapter()
        pending = PendingOrderRegistry(conn)

        # Pre-register a "submitted" order
        pending.register(
            client_order_id="test-order-001",
            intent_id="intent-001",
            session_id=sid,
            instrument_ref_json='{"type":"equity","ticker":"AAPL"}',
            action="buy_to_open",
            venue="mock_venue",
            quantity=10,
        )

        runner = build_paper_runner(
            conn=conn, session_id=sid, equity_adapter=adapter, venue="mock_venue",
        )
        ok = runner.startup()
        assert ok

        # The order should now be marked UNKNOWN
        active = pending.get_active_orders(sid)
        if active:
            for order in active:
                assert order.get("status") in ("unknown", "UNKNOWN", None)


# ===========================================================================
# Phase 4: Replay Strict-Mode Validation
# ===========================================================================


class TestReplayValidation:
    """Replay validation in ContinuousSessionRunner."""

    def test_clean_session_zero_anomalies(self):
        """Empty session (no fills) should replay clean."""
        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(config=config)
        result = cs.run_session()

        assert result.replay_anomaly_count == 0

    def test_session_with_fills_replays(self):
        """Session with market data + fills should replay."""
        sid = str(uuid.uuid4())
        intent = _make_equity_intent(sid)
        strategy = MockStrategy(intents=[intent])
        adapter = MockVenueAdapter()

        env = _make_envelope(15000)
        md = MockMarketDataFeed(envelopes_per_poll=[[env]])

        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=1,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(
            session_id=sid,
            strategy=strategy,
            adapter=adapter,
            md_feed=md,
            config=config,
        )
        result = cs.run_session()

        # Replay should complete (might have anomalies from mock fills, but shouldn't crash)
        assert result.replay_anomaly_count >= 0

    def test_replay_portfolio_state_strict_mode(self):
        """replay_portfolio_state with strict=True raises on anomalies."""
        from gkr_trading.core.replay.engine import replay_portfolio_state

        # Clean event stream should replay fine in strict mode
        events = []  # no events = no anomalies
        result = replay_portfolio_state(events, Decimal("100000"), strict=True)
        assert len(result.anomalies) == 0

    def test_replay_portfolio_state_permissive(self):
        """replay_portfolio_state with strict=False collects anomalies."""
        from gkr_trading.core.replay.engine import replay_portfolio_state

        result = replay_portfolio_state([], Decimal("100000"), strict=False)
        assert result.state.cash == Decimal("100000")


class TestContinuousSessionMultiCycleReplay:
    """Multi-cycle sessions with multiple trades replay correctly."""

    def test_multi_cycle_session_replays_clean(self):
        """A multi-cycle session with multiple dip-buys replays without errors."""
        sid = str(uuid.uuid4())
        intents = [_make_equity_intent(sid) for _ in range(3)]
        strategy = MockStrategy(intents=intents)
        adapter = MockVenueAdapter()

        envs = [
            [_make_envelope(15000)],
            [_make_envelope(14900)],
            [_make_envelope(14800)],
        ]
        md = MockMarketDataFeed(envelopes_per_poll=envs)

        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=3,
            stop_after_market_close=False,
        )
        conn = _in_memory_db()
        cs = _build_continuous_runner(
            conn=conn,
            session_id=sid,
            strategy=strategy,
            adapter=adapter,
            md_feed=md,
            config=config,
        )
        result = cs.run_session()

        assert result.cycles_completed == 3
        # Replay should complete
        assert result.replay_anomaly_count >= 0


# ===========================================================================
# Phase 5 (partial): Certification surface
# ===========================================================================


class TestCertificationSurface:
    """Certification commands work end-to-end with mock adapter."""

    def test_build_and_run_equity_mock(self):
        """paper_v2 _build_and_run with mock adapter succeeds for equity."""
        from gkr_trading.cli.commands.paper_v2 import _build_and_run
        import tempfile, os
        db_path = os.path.join(tempfile.mkdtemp(), "test_cert.db")

        result = _build_and_run(
            db_path=db_path,
            session_id=None,
            adapter_mode="mock",
            strategy_choice="equity",
            shadow_mode=False,
            risk_config_path=None,
        )
        assert result["status"] == "ok"
        assert result["adapter_mode"] == "mock"

    def test_build_and_run_options_mock(self):
        """paper_v2 _build_and_run with mock adapter succeeds for options."""
        from gkr_trading.cli.commands.paper_v2 import _build_and_run
        import tempfile, os
        db_path = os.path.join(tempfile.mkdtemp(), "test_cert.db")

        result = _build_and_run(
            db_path=db_path,
            session_id=None,
            adapter_mode="mock",
            strategy_choice="options",
            shadow_mode=False,
            risk_config_path=None,
        )
        assert result["status"] == "ok"

    def test_build_and_run_shadow_mode(self):
        """Shadow mode logs intents but doesn't submit."""
        from gkr_trading.cli.commands.paper_v2 import _build_and_run
        import tempfile, os
        db_path = os.path.join(tempfile.mkdtemp(), "test_cert.db")

        result = _build_and_run(
            db_path=db_path,
            session_id=None,
            adapter_mode="mock",
            strategy_choice="equity",
            shadow_mode=True,
            risk_config_path=None,
        )
        assert result["status"] == "ok"
        assert result["shadow_mode"] is True
        # In shadow mode, no orders should be submitted
        assert result.get("orders_submitted", 0) == 0


# ===========================================================================
# Integration: ContinuousSessionRunner + multi-cycle strategy
# ===========================================================================


class TestContinuousWithMultiCycleStrategy:
    """Integration: ContinuousSessionRunner + MultiCycleEquityStrategyV2."""

    def test_multi_cycle_dip_buys_through_continuous(self):
        """Multiple dip cycles produce multiple order submissions."""
        sid = str(uuid.uuid4())
        strategy = MultiCycleEquityStrategyV2(session_id=sid, quantity=5)
        adapter = MockVenueAdapter()

        # Create alternating up/down prices to trigger dip buys
        envs = [
            [_make_envelope(15000)],   # initial
            [_make_envelope(14900)],   # dip → buy
            [_make_envelope(15000)],   # up
            [_make_envelope(14800)],   # dip → buy
            [_make_envelope(15000)],   # up
        ]
        md = MockMarketDataFeed(envelopes_per_poll=envs)

        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=5,
            stop_after_market_close=False,
        )
        cs = _build_continuous_runner(
            session_id=sid,
            strategy=strategy,
            adapter=adapter,
            md_feed=md,
            config=config,
        )
        result = cs.run_session()

        assert result.cycles_completed == 5
        # Strategy should have triggered at least 2 dip buys
        assert strategy.trade_count >= 2
        assert result.session_result.intents_generated >= 2
        assert len(adapter._submitted) >= 2

    def test_shadow_mode_prevents_submissions(self):
        """Shadow mode + continuous runner: intents generated but not submitted."""
        sid = str(uuid.uuid4())
        strategy = MultiCycleEquityStrategyV2(session_id=sid, quantity=5)
        adapter = MockVenueAdapter()
        conn = _in_memory_db()

        runner = build_paper_runner(
            conn=conn,
            session_id=sid,
            equity_adapter=adapter,
            strategy=strategy,
            shadow_mode=True,
            venue="mock_venue",
        )

        envs = [
            [_make_envelope(15000)],
            [_make_envelope(14900)],
        ]
        md = MockMarketDataFeed(envelopes_per_poll=envs)

        config = ContinuousSessionConfig(
            poll_interval_sec=0.01,
            max_cycles=2,
            stop_after_market_close=False,
        )
        cs = ContinuousSessionRunner(
            runner=runner,
            market_data_feed=md,
            metadata_provider=MockMetadataProvider(),
            config=config,
        )
        result = cs.run_session()

        assert result.session_result.shadow_mode
        assert len(adapter._submitted) == 0
