"""GKR Trading TUI — main application entry point.

Orchestrates:
  - Background workers (market poller, DB watcher, positions poller)
  - Screen routing
  - Active session state
  - Strategy management
  - Cross-widget data flow

Market-centric design: always-on, sessions are audit trail in background.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Optional

from textual.app import App
from textual.widgets import Static
from textual.worker import Worker, get_current_worker

from gkr_trading.tui.screens.confirm import ConfirmScreen
from gkr_trading.tui.screens.main import MainScreen
from gkr_trading.tui.widgets.event_log import EventLogFooter
from gkr_trading.tui.widgets.history_panel import TradingHistoryPanel
from gkr_trading.tui.widgets.market_table import MarketDataTable, SparklinePanel
from gkr_trading.tui.widgets.operator import (
    KillSwitchPanel,
    ReconciliationPanel,
)
from gkr_trading.tui.widgets.positions import AccountSummaryBar, LivePositionsTable
from gkr_trading.tui.widgets.strategy_panel import (
    AVAILABLE_STRATEGIES,
    DEFAULT_STRATEGY_STATE,
    StrategyAllocationPanel,
    StrategyControlPanel,
)
from gkr_trading.tui.workers.alpaca_positions import (
    AlpacaPositionsWorker,
    LiveAccountSummary,
    LivePosition,
    ALPACA_POSITIONS_POLL_INTERVAL,
)
from gkr_trading.tui.workers.db_watcher import DBWatcher, EventSummary
from gkr_trading.tui.workers.market_poller import MarketPoller

logger = logging.getLogger(__name__)

CSS_PATH = Path(__file__).parent / "styles" / "gkr.tcss"


class GKRTradingApp(App):
    """Interactive terminal dashboard for GKR Trading."""

    TITLE = "GKR Trading"
    CSS_PATH = str(CSS_PATH)

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("ctrl+c", "quit", "Quit"),
        ("l", "toggle_log", "Log"),
    ]

    def __init__(
        self,
        db_path: str,
        initial_session_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._db_path = db_path
        self._active_session_id: Optional[str] = initial_session_id
        self._sessions: list[dict] = []
        self._all_events: list[EventSummary] = []
        self._market_prices: dict[str, int] = {}
        self._market_open: Optional[bool] = None
        self._ks_level = "none"
        self._selected_ticker: Optional[str] = None
        self._db_watcher: Optional[DBWatcher] = None
        self._market_poller: Optional[MarketPoller] = None
        # New: positions worker + strategy state
        self._positions_worker: Optional[AlpacaPositionsWorker] = None
        self._live_positions: list[LivePosition] = []
        self._live_account: Optional[LiveAccountSummary] = None
        self._strategy_states: dict[str, dict] = {
            name: dict(DEFAULT_STRATEGY_STATE) for name in AVAILABLE_STRATEGIES
        }

    def on_mount(self) -> None:
        # Validate DB
        if not Path(self._db_path).exists():
            self.notify(
                f"Database not found: {self._db_path}\n"
                f"Run: init-db --db-path {self._db_path}",
                severity="error",
                timeout=10,
            )

        # Initialize workers
        self._db_watcher = DBWatcher(self._db_path)
        self._market_poller = MarketPoller()
        self._positions_worker = AlpacaPositionsWorker()

        if not self._market_poller.available:
            self.notify(
                "No Alpaca credentials — market data unavailable",
                severity="warning",
                timeout=5,
            )

        if not self._positions_worker.available:
            self.notify(
                "No Alpaca credentials — live positions unavailable",
                severity="warning",
                timeout=5,
            )

        # Install main screen
        self.push_screen(MainScreen())

        # Initial data load
        self.call_after_refresh(self._initial_load)

        # Start background workers
        self.run_worker(self._db_poll_loop, name="db-watcher", thread=True)
        self.run_worker(self._market_poll_loop, name="market-poller", thread=True)
        self.run_worker(self._positions_poll_loop, name="positions-poller", thread=True)

    def _initial_load(self) -> None:
        # Defer until after full mount cycle completes
        self.set_timer(0.5, self._do_initial_load)

    def _do_initial_load(self) -> None:
        self._refresh_sessions()

        # Auto-discover today's session
        if self._active_session_id:
            self.set_active_session(self._active_session_id)
        else:
            today_sid = self._find_todays_session()
            if today_sid:
                self.set_active_session(today_sid)

        # Initial strategy states update
        self._refresh_strategy_panels()

        # Initial history update
        self._refresh_history()

    def _find_todays_session(self) -> Optional[str]:
        """Find the session that was started today."""
        from datetime import date
        today = date.today().isoformat()  # "2026-04-09"
        for s in self._sessions:
            started = s.get("started_at", "")
            if today in started:
                return s["session_id"]
        # Fall back to most recent
        if self._sessions:
            return self._sessions[0]["session_id"]
        return None

    # ── Background workers ──────────────────────────────────────────────

    def _db_poll_loop(self) -> None:
        """Background thread: poll DB for new events every 3s."""
        worker = get_current_worker()
        while not worker.is_cancelled:
            try:
                # Refresh session list
                sessions = self._db_watcher.list_sessions() if self._db_watcher else []
                if sessions != self._sessions:
                    self._sessions = sessions
                    self.call_from_thread(self._update_history_ui)

                # Poll for new events
                if self._active_session_id and self._db_watcher:
                    new_events = self._db_watcher.poll_events(self._active_session_id)
                    if new_events:
                        self._all_events.extend(new_events)
                        self.call_from_thread(self._on_new_events, new_events)
            except Exception as exc:
                logger.error(f"DB watcher error: {exc}")

            time.sleep(3.0)

    def _market_poll_loop(self) -> None:
        """Background thread: poll market data every 15s."""
        worker = get_current_worker()
        if not self._market_poller or not self._market_poller.available:
            return

        while not worker.is_cancelled:
            try:
                snapshots, market_open = self._market_poller.poll_once()
                if snapshots:
                    # Update price cache
                    for s in snapshots:
                        if s.last_cents is not None:
                            self._market_prices[s.ticker] = s.last_cents
                    self.call_from_thread(self._on_market_data, snapshots)

                if market_open is not None and market_open != self._market_open:
                    self._market_open = market_open
                    self.call_from_thread(self._on_market_status, market_open)
            except Exception as exc:
                logger.error(f"Market poller error: {exc}")

            time.sleep(self._market_poller.interval)

    def _positions_poll_loop(self) -> None:
        """Background thread: poll Alpaca positions every 10s."""
        worker = get_current_worker()
        if not self._positions_worker or not self._positions_worker.available:
            return

        while not worker.is_cancelled:
            try:
                positions, account = self._positions_worker.poll_once()
                self.call_from_thread(self._on_positions_update, positions, account)
            except Exception as exc:
                logger.error(f"Positions poller error: {exc}")

            time.sleep(ALPACA_POSITIONS_POLL_INTERVAL)

    # ── UI callbacks (called from main thread via call_from_thread) ─────

    def _on_positions_update(
        self,
        positions: list[LivePosition],
        account: Optional[LiveAccountSummary],
    ) -> None:
        """Handle new live positions data."""
        self._live_positions = positions
        self._live_account = account

        # Update live positions table
        try:
            table = self.query_one("#live-positions-table", LivePositionsTable)
            table.update_positions(positions)
        except Exception:
            pass

        # Update account summary
        try:
            summary = self.query_one("#account-summary", AccountSummaryBar)
            summary.update_summary(account)
        except Exception:
            pass

    def _update_history_ui(self) -> None:
        """Refresh the history panel with session data."""
        self._refresh_history()

    def _on_new_events(self, events: list[EventSummary]) -> None:
        try:
            footer = self.query_one("#event-log-footer", EventLogFooter)
            footer.append_events(events)
        except Exception:
            pass

    def _on_market_data(self, snapshots: list) -> None:
        try:
            table = self.query_one("#market-data", MarketDataTable)
            table.update_data(snapshots)
        except Exception:
            pass

        # Update header ticker tape
        try:
            header = self.query_one("#header-ticker-tape", Static)
            parts = []
            for snap in snapshots:
                if snap.last_cents is not None and snap.open_cents is not None and snap.open_cents > 0:
                    price = snap.last_cents / 100
                    chg = snap.last_cents - snap.open_cents
                    arrow = "▲" if chg >= 0 else "▼"
                    color = "#00e676" if chg >= 0 else "#ff4444"
                    parts.append(f"[bold]{snap.ticker}[/] [{color}]{price:.2f} {arrow}[/]")
                elif snap.last_cents is not None:
                    price = snap.last_cents / 100
                    parts.append(f"[bold]{snap.ticker}[/] {price:.2f}")
            if parts:
                header.update("  ".join(parts))
        except Exception:
            pass

        # Update sparkline if a ticker is selected
        if self._selected_ticker and self._market_poller:
            self.show_ticker_chart(self._selected_ticker)

    def _on_market_status(self, is_open: bool) -> None:
        try:
            screen = self.screen
            if hasattr(screen, "update_header"):
                screen.update_header(market_open=is_open)
        except Exception:
            pass

        # Update logo indicator
        try:
            logo = self.query_one("#header-logo", Static)
            if is_open:
                logo.update("[#00e676]●[/] [bold #ffb300]GKR TRADING[/]")
            else:
                logo.update("[#444444]●[/] [bold #ffb300]GKR TRADING[/]")
        except Exception:
            pass

    # ── Public API (called from screen/widgets) ─────────────────────────

    def set_active_session(self, session_id: str) -> None:
        """Switch active session — refreshes event log and audit data."""
        self._active_session_id = session_id
        self._all_events = []

        # Reset DB watcher seq tracking for this session
        if self._db_watcher:
            self._db_watcher._last_seq[session_id] = 0

        # Load all events for this session
        if self._db_watcher:
            self._all_events = self._db_watcher.get_session_events(session_id)

        # Load event log
        try:
            footer = self.query_one("#event-log-footer", EventLogFooter)
            footer.load_events(self._all_events)
        except Exception:
            pass

        # Update kill switch level
        self._detect_kill_switch_level()

    def show_ticker_chart(self, ticker: str) -> None:
        """Show sparkline chart for a ticker."""
        self._selected_ticker = ticker
        if not self._market_poller:
            return
        try:
            panel = self.query_one("#sparkline-panel", SparklinePanel)
            prices = self._market_poller.get_price_history(ticker)
            if prices:
                panel.update_chart(ticker, prices)
            else:
                panel.update_chart(ticker, [])
        except Exception:
            pass

    def handle_kill_switch(self, level: str) -> None:
        """Handle kill switch button press — confirm then execute."""
        if level == self._ks_level:
            self.notify(f"Kill switch already at {level}", severity="information")
            return

        # Escalation requires confirmation
        if level in ("close_only", "full_halt"):
            sid = self._active_session_id or "none"
            self.push_screen(
                ConfirmScreen(
                    title=f"Activate {level.upper().replace('_', ' ')}?",
                    message=f"This will change the kill switch level to {level.upper()}.",
                    confirm_label=f"Activate {level.upper().replace('_', ' ')}",
                ),
                callback=lambda confirmed: self._execute_kill_switch(level) if confirmed else None,
            )
        else:
            self._execute_kill_switch(level)

    def _execute_kill_switch(self, level: str) -> None:
        """Execute kill switch via subprocess."""
        if not self._active_session_id:
            self.notify("No active session", severity="warning")
            return

        try:
            cmd = [
                sys.executable, "-m", "gkr_trading.cli.main",
                "operator", "kill-switch",
                "--db-path", self._db_path,
                "--session-id", self._active_session_id,
                "--level", level,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self._ks_level = level
                try:
                    panel = self.query_one("#kill-switch-panel", KillSwitchPanel)
                    panel.update_level(level)
                except Exception:
                    pass
                self.notify(f"Kill switch set to {level}", severity="information")
            else:
                self.notify(f"Kill switch error: {result.stderr[:100]}", severity="error")
        except Exception as exc:
            self.notify(f"Kill switch error: {exc}", severity="error")

    def handle_reconcile(self) -> None:
        """Run reconciliation for active session."""
        if not self._active_session_id:
            self.notify("No active session", severity="warning")
            return

        self.run_worker(
            lambda: self._run_reconcile(), name="reconcile", thread=True
        )

    def _run_reconcile(self) -> None:
        """Execute reconciliation in background thread."""
        try:
            from gkr_trading.persistence.db import open_sqlite
            from gkr_trading.persistence.position_store import PositionStore

            conn = open_sqlite(self._db_path)
            ps = PositionStore(conn)

            try:
                from gkr_trading.live.alpaca_config import AlpacaPaperConfig
                from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient
                from gkr_trading.live.traditional.alpaca.alpaca_adapter import (
                    AlpacaPaperEquityAdapter,
                )
                from gkr_trading.live.reconciliation_service import ReconciliationService

                cfg = AlpacaPaperConfig.from_env()
                http = UrllibAlpacaHttpClient(config=cfg)
                adapter = AlpacaPaperEquityAdapter(http)

                recon = ReconciliationService(
                    position_store=ps,
                    adapter=adapter,
                    session_id=self._active_session_id,
                )
                snapshot = recon.reconcile(trigger="tui_manual")

                breaks = []
                for b in snapshot.breaks:
                    breaks.append({
                        "field": b.field,
                        "local_value": b.local_value,
                        "venue_value": b.venue_value,
                        "severity": b.severity,
                    })

                self.call_from_thread(
                    self._show_recon_results, snapshot.status, breaks
                )
            except Exception as exc:
                self.call_from_thread(self._show_recon_error, str(exc))

            conn.close()
        except Exception as exc:
            self.call_from_thread(self._show_recon_error, str(exc))

    def _show_recon_results(self, status: str, breaks: list[dict]) -> None:
        try:
            panel = self.query_one("#recon-panel", ReconciliationPanel)
            panel.show_results(status, breaks)
        except Exception:
            pass

    def _show_recon_error(self, error: str) -> None:
        try:
            panel = self.query_one("#recon-panel", ReconciliationPanel)
            panel.show_error(error)
        except Exception:
            pass
        self.notify(f"Reconciliation error: {error[:80]}", severity="error")

    def handle_replay(self) -> None:
        """Run replay validation for active session."""
        if not self._active_session_id:
            self.notify("No active session", severity="warning")
            return

        self.notify("Running replay validation...", severity="information")
        self.run_worker(
            lambda: self._run_replay(), name="replay", thread=True
        )

    def _run_replay(self) -> None:
        """Execute replay in background thread."""
        try:
            from gkr_trading.persistence.db import open_sqlite
            from gkr_trading.persistence.event_store import SqliteEventStore
            from gkr_trading.core.replay import ReplayEngine
            from gkr_trading.core.schemas.ids import SessionId

            conn = open_sqlite(self._db_path)
            store = SqliteEventStore(conn, init_schema=False)
            eng = ReplayEngine(store, Decimal("100000"))

            sid = SessionId(self._active_session_id)
            result, events = eng.replay_session(sid, strict=False)

            position_count = len(result.state.positions) if hasattr(result.state, "positions") else 0
            cash = result.state.cash if hasattr(result.state, "cash") else Decimal("0")
            anomalies = list(result.anomalies)

            self.call_from_thread(
                self._show_replay_results,
                self._active_session_id,
                cash,
                position_count,
                len(events),
                anomalies,
            )
            conn.close()
        except Exception as exc:
            self.call_from_thread(self._show_replay_error, str(exc))

    def _show_replay_results(
        self, session_id: str, cash: Decimal, pos_count: int, event_count: int, anomalies: list
    ) -> None:
        try:
            screen = self.screen
            if hasattr(screen, "show_replay_results"):
                screen.show_replay_results(session_id, cash, pos_count, event_count, anomalies)
        except Exception:
            pass

    def _show_replay_error(self, error: str) -> None:
        try:
            screen = self.screen
            if hasattr(screen, "show_replay_error"):
                screen.show_replay_error(error)
        except Exception:
            pass
        self.notify(f"Replay error: {error[:80]}", severity="error")

    # ── Strategy management ─────────────────────────────────────────────

    def handle_strategy_toggle(self, strategy_name: str, active: bool) -> None:
        """Toggle a strategy on/off."""
        if strategy_name not in self._strategy_states:
            self._strategy_states[strategy_name] = dict(DEFAULT_STRATEGY_STATE)

        self._strategy_states[strategy_name]["active"] = active

        if active and self._market_open:
            self._launch_strategy_session(strategy_name)
        elif active and not self._market_open:
            self.notify(
                f"Strategy {strategy_name} enabled — will start when market opens",
                severity="information",
            )
        elif not active:
            self.notify(
                f"Strategy {strategy_name} will pause after current cycle",
                severity="information",
            )

        self._refresh_strategy_panels()

    def handle_strategy_allocation_change(self, strategy_name: str, new_pct: int) -> None:
        """Update capital allocation for a strategy."""
        if strategy_name not in self._strategy_states:
            self._strategy_states[strategy_name] = dict(DEFAULT_STRATEGY_STATE)

        # Validate total allocations don't exceed 100%
        total = sum(
            st.get("alloc_pct", 0)
            for name, st in self._strategy_states.items()
            if name != strategy_name
        )
        if total + new_pct > 100:
            self.notify(
                f"Cannot allocate {new_pct}%: total would exceed 100% "
                f"(currently {total}% allocated to other strategies)",
                severity="warning",
            )
            return

        self._strategy_states[strategy_name]["alloc_pct"] = new_pct
        self._refresh_strategy_panels()

    def _launch_strategy_session(self, strategy_name: str) -> None:
        """Launch a strategy session via subprocess."""
        # Map strategy name to CLI --strategy arg
        strategy_arg = strategy_name.replace("_", "-")

        try:
            cmd = [
                sys.executable, "-m", "gkr_trading.cli.main",
                "paper-v2-continuous",
                "--db-path", self._db_path,
                "--strategy", strategy_arg,
                "--max-cycles", "50",
                "--no-websocket",
            ]
            subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.notify(
                f"Strategy {strategy_name} launched in background",
                severity="information",
            )
            # Update state
            if strategy_name in self._strategy_states:
                self._strategy_states[strategy_name]["status"] = "running"
            self._refresh_strategy_panels()
        except Exception as exc:
            self.notify(f"Launch error: {exc}", severity="error")

    def refresh_active_data(self) -> None:
        """Force refresh all data."""
        self._refresh_sessions()
        self._refresh_history()
        self._refresh_strategy_panels()
        if self._active_session_id:
            self.set_active_session(self._active_session_id)

    def action_toggle_log(self) -> None:
        """Toggle event log visibility."""
        try:
            footer = self.query_one("#footer-log")
            footer.toggle_class("hidden")
        except Exception:
            pass

    # ── Internal helpers ────────────────────────────────────────────────

    def _refresh_sessions(self) -> None:
        if self._db_watcher:
            self._sessions = self._db_watcher.list_sessions()

    def _refresh_history(self) -> None:
        """Refresh the history tab with enriched session data."""
        if not self._db_watcher:
            return
        try:
            sessions_with_dates = self._db_watcher.get_sessions_with_dates()
            panel = self.query_one("#trading-history-panel", TradingHistoryPanel)
            panel.update_history(sessions_with_dates)
        except Exception:
            pass

    def _refresh_strategy_panels(self) -> None:
        """Update both strategy panels (allocation sidebar + full control)."""
        try:
            alloc = self.query_one("#strategy-alloc-panel", StrategyAllocationPanel)
            alloc.update_strategy_states(self._strategy_states)
        except Exception:
            pass
        try:
            ctrl = self.query_one("#strategy-control-panel", StrategyControlPanel)
            ctrl.update_strategy_states(self._strategy_states)
        except Exception:
            pass

    def _detect_kill_switch_level(self) -> None:
        """Detect current kill switch level from events."""
        level = "none"
        for ev in reversed(self._all_events):
            if ev.event_type == "operator_command":
                payload = ev.payload_summary.lower()
                if "full_halt" in payload:
                    level = "full_halt"
                    break
                elif "close_only" in payload:
                    level = "close_only"
                    break
                elif "none" in payload:
                    level = "none"
                    break
        self._ks_level = level
        try:
            panel = self.query_one("#kill-switch-panel", KillSwitchPanel)
            panel.update_level(level)
        except Exception:
            pass
