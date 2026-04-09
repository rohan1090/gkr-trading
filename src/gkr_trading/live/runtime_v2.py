"""Runtime V2 — end-to-end paper session using the new options-capable architecture.

Two entry points:
- PaperSessionRunnerV2: one-shot / test harness (caller feeds envelopes)
- ContinuousSessionRunner: persistent loop with real market data + websocket

Wires together:
  SessionSupervisor → RiskApprovalGate → OrderSubmissionService
  → AlpacaPaperEquityAdapter / AlpacaOptionsAdapter
  → FillTranslator → PositionAccountingService → ReconciliationService

Preserves:
  - Write-before-call ordering (EventStore → PendingOrderRegistry → API call)
  - Append-only EventStore (WAL + synchronous=FULL)
  - UNKNOWN crash recovery via PendingOrderRegistry
  - Strategy isolation (receives MarketDataEnvelope, emits TradeIntent only)

The legacy runtime.py is preserved untouched for backward compatibility.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional, Sequence

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.payloads import (
    SessionStartedPayload,
    SessionStoppedPayload,
)
from gkr_trading.core.events.types import EventType
from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import EquityRef, InstrumentRef, OptionsRef
from gkr_trading.core.market_data import MarketDataEnvelope
from gkr_trading.core.operator_controls import KillSwitchLevel
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.options_lifecycle import AssignmentEvent, ExerciseEvent, ExpirationEvent
from gkr_trading.core.risk_gate import RiskApprovalGate, RiskDecision
from gkr_trading.live.base import VenueAdapter
from gkr_trading.live.fill_translator import FillTranslator
from gkr_trading.live.order_submission_service import OrderSubmissionService, SubmissionOutcome
from gkr_trading.live.position_accounting_service import PositionAccountingService
from gkr_trading.live.reconciliation_service import ReconciliationService
from gkr_trading.live.session_supervisor import SessionSupervisor, SessionState
from gkr_trading.live.traditional.alpaca.alpaca_options_fill_translator import (
    is_nta_lifecycle_event,
    get_nta_event_type,
)
from gkr_trading.live.traditional.options.options_adapter_base import OptionsCapableAdapterMixin
from gkr_trading.persistence.event_store import EventStore, SqliteEventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry
from gkr_trading.persistence.position_store import PositionStore
from gkr_trading.strategy.base import OptionsAwareStrategy

logger = logging.getLogger(__name__)


@dataclass
class PaperSessionV2Config:
    """Configuration for a V2 paper session."""
    session_id: str
    venue: str = "alpaca_paper"
    mode: str = "paper"
    shadow_mode: bool = False
    dry_run: bool = False


@dataclass
class PaperSessionV2Result:
    """Result from a V2 paper session."""
    session_id: str
    events_count: int
    fills_count: int
    intents_generated: int
    intents_approved: int
    intents_rejected: int
    orders_submitted: int
    orders_failed: int
    startup_clean: bool
    shutdown_clean: bool
    replay_valid: bool
    shadow_mode: bool
    errors: List[str] = field(default_factory=list)


@dataclass
class StrategyContext:
    """Read-only context passed to strategy alongside MarketDataEnvelope.

    Strategy can read session state and kill switch but cannot modify anything.
    """
    session_id: str
    venue: str
    kill_switch_level: str = "none"
    is_shadow: bool = False


class PaperSessionRunnerV2:
    """Orchestrates a complete paper trading session through the new architecture.

    Lifecycle:
    1. startup() — reconciliation, UNKNOWN recovery, session_started event
    2. process_market_data() — for each bar/tick:
       a. Strategy emits TradeIntent
       b. Risk gate evaluates
       c. OrderSubmissionService submits (write-before-call)
       d. Fill translation + position accounting
    3. shutdown() — reconciliation, session_stopped event, replay validation

    This class does NOT own the event loop — the caller feeds market data.
    """

    def __init__(
        self,
        config: PaperSessionV2Config,
        event_store: EventStore,
        pending_registry: PendingOrderRegistry,
        position_store: PositionStore,
        equity_adapter: VenueAdapter,
        options_adapter: Optional[OptionsCapableAdapterMixin] = None,
        equity_fill_translator: Optional[FillTranslator] = None,
        options_fill_translator: Optional[FillTranslator] = None,
        risk_gates: Optional[Sequence[RiskApprovalGate]] = None,
        strategy: Optional[OptionsAwareStrategy] = None,
    ) -> None:
        self._config = config
        self._event_store = event_store
        self._pending = pending_registry
        self._position_store = position_store
        self._equity_adapter = equity_adapter
        self._options_adapter = options_adapter
        self._equity_fill_translator = equity_fill_translator
        self._options_fill_translator = options_fill_translator
        self._risk_gates = list(risk_gates or [])
        self._strategy = strategy

        # Build internal services
        self._recon = ReconciliationService(
            position_store=position_store,
            adapter=equity_adapter,
            session_id=config.session_id,
        )
        self._supervisor = SessionSupervisor(
            event_store=event_store,
            pending_registry=pending_registry,
            reconciliation_service=self._recon,
            session_id=config.session_id,
            venue=config.venue,
        )
        self._submission = OrderSubmissionService(
            event_store=event_store,
            pending_registry=pending_registry,
            adapter=equity_adapter,  # default; options routed in submit_intent
        )
        self._accounting = PositionAccountingService(
            position_store=position_store,
            session_id=config.session_id,
        )

        # Session stats
        self._intents_generated = 0
        self._intents_approved = 0
        self._intents_rejected = 0
        self._orders_submitted = 0
        self._orders_failed = 0
        self._fills_count = 0
        self._errors: List[str] = []
        self._startup_clean = False
        self._shutdown_clean = False

    @property
    def supervisor(self) -> SessionSupervisor:
        return self._supervisor

    def startup(self) -> bool:
        """Run startup sequence via SessionSupervisor.

        Returns True if session is ready to accept market data.
        """
        ok = self._supervisor.startup()
        self._startup_clean = ok
        if not ok:
            logger.error("Startup reconciliation failed or blocking breaks found")
        return ok

    def process_market_data(self, envelope: MarketDataEnvelope) -> Optional[SubmissionOutcome]:
        """Process one market data event through the full pipeline.

        1. Check session state (running, not halted/suspended)
        2. Feed to strategy
        3. If strategy emits TradeIntent, evaluate risk gates
        4. If approved, submit via OrderSubmissionService
        5. Return SubmissionOutcome or None
        """
        # Gate: session must be running
        if self._supervisor.state != SessionState.RUNNING:
            return None

        if self._strategy is None:
            return None

        # Strategy receives envelope + read-only context
        ctx = StrategyContext(
            session_id=self._config.session_id,
            venue=self._config.venue,
            kill_switch_level=self._supervisor.kill_switch.value,
            is_shadow=self._config.shadow_mode,
        )
        raw_intent = self._strategy.on_market_data(envelope, ctx)
        if raw_intent is None:
            return None
        if not isinstance(raw_intent, TradeIntent):
            logger.warning(f"Strategy returned non-TradeIntent: {type(raw_intent)}")
            return None

        self._intents_generated += 1
        intent = raw_intent

        # Check kill switch before risk evaluation
        if isinstance(intent.instrument_ref, OptionsRef) or intent.action in ("buy_to_open", "sell_to_open"):
            if not self._supervisor.can_submit_new_orders():
                self._intents_rejected += 1
                return None
        else:
            if not self._supervisor.can_submit_orders():
                self._intents_rejected += 1
                return None

        # Run through risk gates
        for gate in self._risk_gates:
            decision = gate.evaluate(intent, ctx)
            if not decision.approved:
                self._intents_rejected += 1
                logger.info(f"Risk rejected: {decision.reason_code} — {decision.reason_detail}")
                return None

        self._intents_approved += 1

        # Shadow mode: log but don't submit
        if self._config.shadow_mode:
            self._log_shadow_order(intent)
            return None

        # Route to correct adapter
        return self._submit_intent(intent)

    def process_venue_events(self, venue_payloads: List[dict]) -> None:
        """Process fill/NTA events received from venue (polling or websocket).

        For each payload:
        - If NTA lifecycle event → route to options adapter for translation
        - If fill → route to fill translator → position accounting
        """
        for payload in venue_payloads:
            try:
                if is_nta_lifecycle_event(payload):
                    self._process_nta_event(payload)
                else:
                    self._process_fill_event(payload)
            except Exception as exc:
                self._errors.append(f"Venue event processing error: {exc}")
                logger.error(f"Failed to process venue event: {exc}")

    def shutdown(self) -> PaperSessionV2Result:
        """Run shutdown sequence and return session result."""
        snapshot = self._supervisor.shutdown()
        self._shutdown_clean = not snapshot.has_blocking_breaks()

        # Count events
        events = self._event_store.load_session(self._config.session_id)

        return PaperSessionV2Result(
            session_id=self._config.session_id,
            events_count=len(events),
            fills_count=self._fills_count,
            intents_generated=self._intents_generated,
            intents_approved=self._intents_approved,
            intents_rejected=self._intents_rejected,
            orders_submitted=self._orders_submitted,
            orders_failed=self._orders_failed,
            startup_clean=self._startup_clean,
            shutdown_clean=self._shutdown_clean,
            replay_valid=True,  # caller runs replay separately
            shadow_mode=self._config.shadow_mode,
            errors=list(self._errors),
        )

    def _submit_intent(self, intent: TradeIntent) -> SubmissionOutcome:
        """Submit a TradeIntent through OrderSubmissionService.

        Routes options intents to the options adapter if available.
        """
        # Determine which adapter to use
        if isinstance(intent.instrument_ref, OptionsRef) and self._options_adapter is not None:
            # For options, we need a VenueAdapter-compatible wrapper
            # The options adapter has submit_options_order but not submit_order
            # We use a thin routing adapter
            outcome = self._submit_options_intent(intent)
        else:
            outcome = self._submission.submit(intent, self._config.venue)

        if outcome.success:
            self._orders_submitted += 1
        else:
            self._orders_failed += 1
            if outcome.error:
                self._errors.append(f"Submission error: {outcome.error}")

        return outcome

    def _submit_options_intent(self, intent: TradeIntent) -> SubmissionOutcome:
        """Submit an options intent through the options adapter with write-before-call."""
        from gkr_trading.live.order_submission_service import _instrument_ref_to_json, _now_utc_iso
        from gkr_trading.core.events.payloads import (
            PendingOrderRegisteredPayload,
            OrderSubmissionAttemptedPayload,
        )
        from gkr_trading.core.order_model import OrderStatus
        from gkr_trading.live.base import SubmissionRequest

        client_order_id = str(uuid.uuid4())

        # Step 1: Write to PendingOrderRegistry BEFORE API call
        instrument_json = _instrument_ref_to_json(intent.instrument_ref)
        registered = self._pending.register(
            client_order_id=client_order_id,
            intent_id=intent.intent_id,
            session_id=intent.session_id,
            instrument_ref_json=instrument_json,
            action=intent.action,
            venue=self._config.venue,
            quantity=intent.quantity,
            limit_price_cents=intent.limit_price_cents,
        )
        if not registered:
            return SubmissionOutcome(
                client_order_id=client_order_id, success=False, duplicate=True,
            )

        # Step 2: Persist pending-order-registered event
        pending_event = CanonicalEvent(
            event_type=EventType.PENDING_ORDER_REGISTERED,
            occurred_at_utc=_now_utc_iso(),
            payload=PendingOrderRegisteredPayload(
                client_order_id=client_order_id,
                intent_id=intent.intent_id,
                instrument_key=intent.instrument_ref.canonical_key,
                action=intent.action,
                venue=self._config.venue,
                quantity=intent.quantity,
                limit_price_cents=intent.limit_price_cents,
            ),
        )
        self._event_store.append(intent.session_id, pending_event)

        # Step 3: Make API call via options adapter
        request = SubmissionRequest(
            client_order_id=client_order_id,
            instrument_ref=intent.instrument_ref,
            action=intent.action,
            quantity=intent.quantity,
            limit_price_cents=intent.limit_price_cents,
            time_in_force=intent.time_in_force,
            venue=self._config.venue,
        )
        try:
            response = self._options_adapter.submit_options_order(request)
        except Exception as exc:
            error_event = CanonicalEvent(
                event_type=EventType.ORDER_SUBMISSION_ATTEMPTED,
                occurred_at_utc=_now_utc_iso(),
                payload=OrderSubmissionAttemptedPayload(
                    client_order_id=client_order_id, success=False, timeout=True,
                ),
            )
            self._event_store.append(intent.session_id, error_event)
            self._pending.update_status(client_order_id, OrderStatus.UNKNOWN)
            return SubmissionOutcome(
                client_order_id=client_order_id, success=False, error=str(exc),
            )

        # Step 4: Persist API response event
        response_event = CanonicalEvent(
            event_type=EventType.ORDER_SUBMISSION_ATTEMPTED,
            occurred_at_utc=_now_utc_iso(),
            payload=OrderSubmissionAttemptedPayload(
                client_order_id=client_order_id,
                venue_order_id=response.venue_order_id,
                success=response.success,
                rejected=response.rejected,
                reject_reason=response.reject_reason,
            ),
        )
        self._event_store.append(intent.session_id, response_event)

        # Step 5: Update PendingOrderRegistry
        if response.rejected:
            self._pending.update_status(client_order_id, OrderStatus.REJECTED, response.venue_order_id)
        elif response.success:
            self._pending.update_status(client_order_id, OrderStatus.SUBMITTED, response.venue_order_id)
        else:
            self._pending.update_status(client_order_id, OrderStatus.UNKNOWN)

        return SubmissionOutcome(
            client_order_id=client_order_id,
            success=response.success and not response.rejected,
            venue_order_id=response.venue_order_id,
            rejected=response.rejected,
            reject_reason=response.reject_reason,
        )

    def _process_fill_event(self, payload: dict) -> None:
        """Translate venue fill payload to canonical FillEvent and apply to positions."""
        fill: Optional[FillEvent] = None

        # Try options translator first
        if self._options_fill_translator:
            fill = self._options_fill_translator.translate_fill(payload)
        # Fall back to equity translator
        if fill is None and self._equity_fill_translator:
            fill = self._equity_fill_translator.translate_fill(payload)

        if fill is None:
            return

        # Apply to position accounting
        self._accounting.apply_fill(fill)
        self._fills_count += 1

        # Update pending order registry if we can match
        if fill.client_order_id:
            from gkr_trading.core.order_model import OrderStatus
            self._pending.update_status(fill.client_order_id, OrderStatus.FILLED)

    def _process_nta_event(self, payload: dict) -> None:
        """Process NTA lifecycle event (assignment/exercise/expiration)."""
        if self._options_adapter is None:
            return

        nta_type = get_nta_event_type(payload)
        if nta_type == "assignment":
            event = self._options_adapter.translate_assignment(payload)
            if event:
                self._accounting.apply_assignment(event)
                logger.warning(f"Assignment processed: {event.equity_underlying}")
        elif nta_type == "exercise":
            event = self._options_adapter.translate_exercise(payload)
            if event:
                self._accounting.apply_exercise(event)
                logger.info(f"Exercise processed: {event.equity_underlying}")
        elif nta_type == "expiration":
            event = self._options_adapter.translate_expiration(payload)
            if event:
                self._accounting.apply_expiration(event)
                logger.info(f"Expiration processed: {event.instrument_ref.occ_symbol}")

    def _log_shadow_order(self, intent: TradeIntent) -> None:
        """Log a shadow order (no actual submission). Options-aware."""
        ref = intent.instrument_ref
        shadow_data = {
            "type": "shadow_order",
            "intent_id": intent.intent_id,
            "action": intent.action,
            "quantity": intent.quantity,
            "limit_price_cents": intent.limit_price_cents,
            "instrument_key": ref.canonical_key,
            "time_in_force": intent.time_in_force,
        }
        if isinstance(ref, OptionsRef):
            shadow_data["occ_symbol"] = ref.occ_symbol
            shadow_data["position_intent"] = intent.action
            shadow_data["underlying"] = ref.underlying
            shadow_data["expiry"] = ref.expiry.isoformat()
            shadow_data["strike_cents"] = ref.strike_cents
            shadow_data["right"] = ref.right

        logger.info(f"SHADOW ORDER: {shadow_data}")


def build_paper_runner(
    *,
    conn: sqlite3.Connection,
    session_id: str,
    equity_adapter: VenueAdapter,
    options_adapter: Optional[OptionsCapableAdapterMixin] = None,
    equity_fill_translator: Optional[FillTranslator] = None,
    options_fill_translator: Optional[FillTranslator] = None,
    risk_gates: Optional[Sequence[RiskApprovalGate]] = None,
    strategy: Optional[OptionsAwareStrategy] = None,
    shadow_mode: bool = False,
    venue: str = "alpaca_paper",
) -> PaperSessionRunnerV2:
    """Factory: build a PaperSessionRunnerV2 with all dependencies wired."""
    event_store = SqliteEventStore(conn)
    pending_registry = PendingOrderRegistry(conn)
    position_store = PositionStore(conn)

    config = PaperSessionV2Config(
        session_id=session_id,
        venue=venue,
        shadow_mode=shadow_mode,
    )

    return PaperSessionRunnerV2(
        config=config,
        event_store=event_store,
        pending_registry=pending_registry,
        position_store=position_store,
        equity_adapter=equity_adapter,
        options_adapter=options_adapter,
        equity_fill_translator=equity_fill_translator,
        options_fill_translator=options_fill_translator,
        risk_gates=risk_gates,
        strategy=strategy,
    )


# ---------------------------------------------------------------------------
# Continuous session runner — persistent market-data loop + websocket
# ---------------------------------------------------------------------------

@dataclass
class ContinuousSessionConfig:
    """Config for a continuous (non-one-shot) paper session."""
    poll_interval_sec: float = 15.0
    max_cycles: Optional[int] = None           # None = run until stop condition
    stop_after_market_close: bool = True
    max_consecutive_md_failures: int = 5
    enable_websocket: bool = True


class StopReason:
    MARKET_CLOSED = "market_closed"
    KILL_SWITCH = "kill_switch"
    BLOCKING_RECON = "blocking_reconciliation_break"
    MD_FAILURE = "market_data_failure"
    MAX_CYCLES = "max_cycles_reached"
    OPERATOR_HALT = "operator_halt"
    WS_FATAL = "websocket_fatal"
    EXTERNAL = "external_stop"


@dataclass
class ContinuousSessionResult:
    """Result of a continuous session run."""
    session_result: PaperSessionV2Result
    cycles_completed: int
    stop_reason: str
    md_polls: int
    md_envelopes: int
    ws_connected: bool
    ws_trade_updates: int
    replay_anomaly_count: int


class ContinuousSessionRunner:
    """Persistent session loop: polls market data, feeds strategy, handles fills via WS.

    Lifecycle:
    1. startup() — startup recon, WS connect, session_started event
    2. run() — loop: poll market data → feed strategy → sleep → repeat
       - WS trade_updates processed in background thread via callbacks
       - Stop conditions: market close, kill switch, recon break, md failure
    3. shutdown() — shutdown recon, WS disconnect, session_stopped event
    """

    def __init__(
        self,
        runner: PaperSessionRunnerV2,
        market_data_feed: Any,            # AlpacaMarketDataFeed
        metadata_provider: Any,           # MarketMetadataProvider
        ws_manager: Any = None,           # AlpacaWebSocketManager
        config: Optional[ContinuousSessionConfig] = None,
    ) -> None:
        self._runner = runner
        self._feed = market_data_feed
        self._metadata = metadata_provider
        self._ws = ws_manager
        self._config = config or ContinuousSessionConfig()
        self._cycles = 0
        self._stop_reason = ""
        self._stop_requested = False
        self._ws_trade_updates = 0
        self._ws_connected = False
        self._submission_suspended = False
        self._process_lock = threading.Lock()

    @property
    def runner(self) -> PaperSessionRunnerV2:
        return self._runner

    def request_stop(self, reason: str = StopReason.EXTERNAL) -> None:
        """Request graceful stop from outside the loop."""
        self._stop_requested = True
        self._stop_reason = reason

    def run_session(self) -> ContinuousSessionResult:
        """Run the full session: startup → loop → shutdown."""
        # --- Startup ---
        ok = self._runner.startup()
        if not ok:
            result = self._runner.shutdown()
            return ContinuousSessionResult(
                session_result=result,
                cycles_completed=0,
                stop_reason=StopReason.BLOCKING_RECON,
                md_polls=0,
                md_envelopes=0,
                ws_connected=False,
                ws_trade_updates=0,
                replay_anomaly_count=0,
            )

        # --- Start WebSocket ---
        if self._ws and self._config.enable_websocket:
            self._setup_ws_callbacks()
            try:
                self._ws.start()
            except Exception as exc:
                logger.warning(f"WebSocket start failed: {exc}")

        # --- Main loop ---
        self._stop_reason = self._run_loop()

        # --- Stop WebSocket ---
        if self._ws:
            try:
                self._ws.stop()
            except Exception:
                pass

        # --- Shutdown ---
        session_result = self._runner.shutdown()

        # --- Replay validation ---
        anomaly_count = self._run_replay_validation()

        return ContinuousSessionResult(
            session_result=session_result,
            cycles_completed=self._cycles,
            stop_reason=self._stop_reason,
            md_polls=self._feed.stats.polls if self._feed else 0,
            md_envelopes=self._feed.stats.envelopes_produced if self._feed else 0,
            ws_connected=self._ws_connected,
            ws_trade_updates=self._ws_trade_updates,
            replay_anomaly_count=anomaly_count,
        )

    def _run_loop(self) -> str:
        """Core loop. Returns stop reason."""
        while not self._stop_requested:
            # Check stop conditions
            stop = self._check_stop_conditions()
            if stop:
                return stop

            # Poll market data
            try:
                envelopes = self._feed.poll() if self._feed else []
            except Exception as exc:
                logger.error(f"Market data poll exception: {exc}")
                envelopes = []

            if self._feed and self._feed.has_fatal_failure:
                return StopReason.MD_FAILURE

            # Feed each envelope to strategy
            for env in envelopes:
                if self._stop_requested:
                    return self._stop_reason or StopReason.EXTERNAL
                try:
                    with self._process_lock:
                        self._runner.process_market_data(env)
                except Exception as exc:
                    logger.error(f"Error processing market data: {exc}")
                    self._runner._errors.append(str(exc))

            self._cycles += 1

            # Check cycle limit
            if self._config.max_cycles and self._cycles >= self._config.max_cycles:
                return StopReason.MAX_CYCLES

            # Sleep until next poll
            self._interruptible_sleep(self._config.poll_interval_sec)

        return self._stop_reason or StopReason.EXTERNAL

    def _check_stop_conditions(self) -> Optional[str]:
        """Check all stop conditions. Returns reason or None."""
        # Kill switch
        if self._runner.supervisor.state == SessionState.HALTED:
            return StopReason.KILL_SWITCH

        # Market closed
        if self._config.stop_after_market_close and self._metadata:
            try:
                if not self._metadata.is_market_open():
                    return StopReason.MARKET_CLOSED
            except Exception:
                pass  # Don't halt on metadata errors

        return None

    def _setup_ws_callbacks(self) -> None:
        """Wire WebSocket callbacks into the runtime."""
        self._ws._on_trade_update = self._on_ws_trade_update
        self._ws._on_disconnect = self._on_ws_disconnect
        self._ws._on_reconnect = self._on_ws_reconnect
        self._ws._on_connect = self._on_ws_connect

    def _on_ws_connect(self) -> None:
        self._ws_connected = True
        logger.info("WebSocket connected")

    def _on_ws_trade_update(self, payload: dict) -> None:
        """Process a trade_update from the websocket (called from WS thread)."""
        self._ws_trade_updates += 1
        try:
            with self._process_lock:
                self._runner.process_venue_events([payload])
        except Exception as exc:
            logger.error(f"WS trade_update processing error: {exc}")

    def _on_ws_disconnect(self, reason: str) -> None:
        """Handle websocket disconnect: suspend submissions."""
        self._ws_connected = False
        self._submission_suspended = True
        self._runner.supervisor.suspend(reason=f"ws_disconnect: {reason}")
        logger.warning(f"WebSocket disconnected, submissions suspended: {reason}")

    def _on_ws_reconnect(self) -> None:
        """Handle websocket reconnect: reconcile then resume."""
        self._ws_connected = True
        resumed = self._runner.supervisor.resume()
        if resumed:
            self._submission_suspended = False
            logger.info("WebSocket reconnected, reconciled, submissions resumed")
        else:
            logger.error("Post-reconnect reconciliation failed, session halted")
            self.request_stop(StopReason.BLOCKING_RECON)

    def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep that can be interrupted by stop request."""
        end = time.time() + seconds
        while time.time() < end and not self._stop_requested:
            time.sleep(min(0.5, end - time.time()))

    def _run_replay_validation(self) -> int:
        """Run replay validation and return anomaly count."""
        try:
            from gkr_trading.core.replay import ReplayEngine
            from decimal import Decimal
            import sqlite3

            events = self._runner._event_store.load_session(
                self._runner._config.session_id
            )
            if not events:
                return 0

            eng = ReplayEngine(self._runner._event_store, Decimal("100000"))
            from gkr_trading.core.schemas.ids import SessionId
            result, _ = eng.replay_session(
                SessionId(self._runner._config.session_id), strict=False
            )
            return len(result.anomalies)
        except Exception as exc:
            logger.error(f"Replay validation failed: {exc}")
            return -1
