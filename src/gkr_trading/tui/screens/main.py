"""Primary 4-tab layout screen for GKR Trading TUI.

Tabs: Positions | Market | Strategies | History

Market-centric design: always-on, no session UUID visible.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Label,
    Static,
    TabbedContent,
    TabPane,
)

from gkr_trading.tui.widgets.event_log import EventLogFooter
from gkr_trading.tui.widgets.history_panel import ReplayRequested, TradingHistoryPanel
from gkr_trading.tui.widgets.market_table import MarketDataTable, SparklinePanel, TickerSelected
from gkr_trading.tui.widgets.operator import (
    KillSwitchAction,
    KillSwitchPanel,
    ReconcileAction,
    ReconciliationPanel,
)
from gkr_trading.tui.widgets.positions import AccountSummaryBar, LivePositionsTable
from gkr_trading.tui.widgets.session_list import SessionSelected
from gkr_trading.tui.widgets.strategy_panel import (
    StrategyAllocationChanged,
    StrategyAllocationPanel,
    StrategyControlPanel,
    StrategyPauseRequested,
    StrategyStartRequested,
    StrategyToggled,
)

logger = logging.getLogger(__name__)


class MainScreen(Screen):
    """Primary screen with header, 4 tabs, and footer event log."""

    BINDINGS = [
        ("1", "switch_tab('tab-positions')", "Positions"),
        ("2", "switch_tab('tab-market')", "Market"),
        ("3", "switch_tab('tab-strategies')", "Strategies"),
        ("4", "switch_tab('tab-history')", "History"),
        ("r", "refresh_tab", "Refresh"),
        ("d", "toggle_dark", "Theme"),
        ("q", "request_quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        # ── Header bar ──
        with Horizontal(id="header-bar"):
            yield Static("[bold #ffb300]● GKR TRADING[/]", id="header-logo")
            yield Static("[#444444]waiting for market data...[/]", id="header-ticker-tape")
            yield Static("[#444444]--:--:-- ET[/]", id="header-right")

        # ── Tabbed content ──
        with TabbedContent(id="main-tabs"):
            # Tab 1: Positions (DEFAULT — shown on launch)
            with TabPane("Positions", id="tab-positions"):
                with Horizontal(classes="split-h"):
                    # Left 65%: live positions from Alpaca API
                    with Vertical(classes="left-65 panel"):
                        yield Label("LIVE POSITIONS", classes="section-label")
                        yield LivePositionsTable(id="live-positions-table")
                        yield AccountSummaryBar(id="account-summary")
                    # Right 35%: strategy allocation sidebar
                    with Vertical(classes="right-35 panel"):
                        yield Label("STRATEGIES", classes="section-label")
                        yield StrategyAllocationPanel(id="strategy-alloc-panel")

            # Tab 2: Market
            with TabPane("Market", id="tab-market"):
                with Horizontal(classes="split-h"):
                    with Vertical(classes="left-35 panel"):
                        yield Label("WATCHLIST", classes="section-label")
                        yield MarketDataTable(id="market-data")
                    with Vertical(classes="right-65 panel"):
                        yield Label("PRICE CHART", classes="section-label")
                        yield SparklinePanel(id="sparkline-panel")

            # Tab 3: Strategies
            with TabPane("Strategies", id="tab-strategies"):
                with Vertical(classes="panel scroll-panel"):
                    yield Label("ACTIVE STRATEGIES", classes="section-label")
                    yield StrategyControlPanel(id="strategy-control-panel")
                    yield Label("SYSTEM CONTROLS", classes="section-label")
                    yield KillSwitchPanel(id="kill-switch-panel")
                    yield ReconciliationPanel(id="recon-panel")

            # Tab 4: History
            with TabPane("History", id="tab-history"):
                with Vertical(classes="panel"):
                    yield Label("TRADING HISTORY", classes="section-label")
                    yield TradingHistoryPanel(id="trading-history-panel")
                    yield Label("REPLAY", classes="section-label")
                    with Horizontal():
                        yield Button(
                            "Run Replay",
                            id="btn-replay",
                            classes="btn-accent",
                        )
                        yield Static("", id="replay-session-label")
                    yield Static("", id="replay-results")
                    yield DataTable(id="anomaly-table", cursor_type="none")

        # ── Footer event log ──
        with Vertical(id="footer-log"):
            yield EventLogFooter(id="event-log-footer")

    def on_mount(self) -> None:
        # Setup anomaly table columns
        table = self.query_one("#anomaly-table", DataTable)
        table.add_columns("Index", "Event Type", "Code", "Message")
        table.zebra_stripes = True

        # Start live clock
        self.set_interval(1.0, self._tick_clock)

    def _tick_clock(self) -> None:
        from datetime import datetime
        import zoneinfo
        et = zoneinfo.ZoneInfo("America/New_York")
        now = datetime.now(et)
        time_str = now.strftime("%H:%M:%S ET")
        try:
            right = self.query_one("#header-right", Static)
            current = str(right.renderable)
            if "OPEN" in current:
                right.update(f"[#00e676 bold]OPEN[/]  [#666666]{time_str}[/]")
            elif "CLOSED" in current:
                right.update(f"[#444444]CLOSED  {time_str}[/]")
            else:
                right.update(f"[#666666]{time_str}[/]")
        except Exception:
            pass

    # ── Tab switching ──

    def action_switch_tab(self, tab_id: str) -> None:
        tabs = self.query_one("#main-tabs", TabbedContent)
        tabs.active = tab_id

    def action_toggle_dark(self) -> None:
        self.app.dark = not self.app.dark

    def action_request_quit(self) -> None:
        self.app.exit()

    def action_refresh_tab(self) -> None:
        """Force refresh of the active tab's data."""
        self.app.refresh_active_data()

    # ── Header updates ──

    def update_header(
        self,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        market_open: Optional[bool] = None,
    ) -> None:
        if market_open is not None:
            right = self.query_one("#header-right", Static)
            if market_open:
                right.update("[#00e676 bold]OPEN[/]")
            else:
                right.update("[#444444]CLOSED[/]")

    # ── Message routing to app ──

    def on_session_selected(self, event: SessionSelected) -> None:
        self.app.set_active_session(event.session_id)

    def on_ticker_selected(self, event: TickerSelected) -> None:
        self.app.show_ticker_chart(event.ticker)

    def on_kill_switch_action(self, event: KillSwitchAction) -> None:
        self.app.handle_kill_switch(event.level)

    def on_reconcile_action(self, event: ReconcileAction) -> None:
        self.app.handle_reconcile()

    def on_strategy_toggled(self, event: StrategyToggled) -> None:
        self.app.handle_strategy_toggle(event.strategy_name, event.active)

    def on_strategy_allocation_changed(self, event: StrategyAllocationChanged) -> None:
        self.app.handle_strategy_allocation_change(event.strategy_name, event.new_pct)

    def on_strategy_start_requested(self, event: StrategyStartRequested) -> None:
        self.app._launch_strategy_session(event.strategy_name)

    def on_strategy_pause_requested(self, event: StrategyPauseRequested) -> None:
        self.app.notify(
            f"Strategy {event.strategy_name} will pause after current cycle",
            severity="information",
        )

    def on_replay_requested(self, event: ReplayRequested) -> None:
        self.app.set_active_session(event.session_id)
        self.app.handle_replay()

    # ── Button press routing ──

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-replay":
            self.app.handle_replay()

    # ── Replay results display ──

    def show_replay_results(
        self,
        session_id: str,
        cash: Decimal,
        position_count: int,
        event_count: int,
        anomalies: list,
    ) -> None:
        try:
            label = self.query_one("#replay-session-label", Static)
            label.update(f"  Session: {session_id[:20]}...")
        except Exception:
            pass

        try:
            results = self.query_one("#replay-results", Static)
            anomaly_count = len(anomalies)
            if anomaly_count == 0:
                badge = "[#00e676 bold]0 anomalies[/]"
            elif anomaly_count <= 5:
                badge = f"[#ffb300 bold]{anomaly_count} anomalies[/]"
            else:
                badge = f"[#ff4444 bold]{anomaly_count} anomalies[/]"

            results.update(
                f"[bold]Cash:[/] ${cash:,.2f}  |  "
                f"[bold]Positions:[/] {position_count}  |  "
                f"[bold]Events replayed:[/] {event_count}  |  "
                f"{badge}"
            )
        except Exception:
            pass

        # Fill anomaly table
        try:
            table = self.query_one("#anomaly-table", DataTable)
            table.clear()
            for a in anomalies:
                code = getattr(a, "code", str(a)) if hasattr(a, "code") else "?"
                msg = getattr(a, "message", str(a)) if hasattr(a, "message") else str(a)
                idx = getattr(a, "event_index", "?") if hasattr(a, "event_index") else "?"
                color = "#ff4444" if "error" in str(code).lower() else "#ffb300"
                table.add_row(
                    str(idx),
                    str(getattr(a, "event_type", "—")),
                    f"[{color}]{code}[/]",
                    msg[:60],
                )
        except Exception:
            pass

    def show_replay_error(self, error: str) -> None:
        try:
            results = self.query_one("#replay-results", Static)
            results.update(f"[#ff4444]Replay error: {error}[/]")
        except Exception:
            pass
