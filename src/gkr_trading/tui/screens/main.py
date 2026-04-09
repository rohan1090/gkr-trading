"""Primary 5-tab layout screen for GKR Trading TUI."""
from __future__ import annotations

import logging
import subprocess
import sys
from decimal import Decimal
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Label,
    Static,
    TabbedContent,
    TabPane,
)

from gkr_trading.tui.widgets.event_log import EventLogFooter
from gkr_trading.tui.widgets.market_table import MarketDataTable, SparklinePanel, TickerSelected
from gkr_trading.tui.widgets.operator import (
    AlertsPanel,
    KillSwitchAction,
    KillSwitchPanel,
    ReconcileAction,
    ReconciliationPanel,
)
from gkr_trading.tui.widgets.positions import EquityPositionsTable, OptionsPositionsTable
from gkr_trading.tui.widgets.session_list import (
    SessionDetailPanel,
    SessionListPanel,
    SessionSelected,
)

logger = logging.getLogger(__name__)


class MainScreen(Screen):
    """Primary screen with header, 5 tabs, and footer event log."""

    BINDINGS = [
        ("1", "switch_tab('tab-sessions')", "Sessions"),
        ("2", "switch_tab('tab-positions')", "Positions"),
        ("3", "switch_tab('tab-market')", "Market"),
        ("4", "switch_tab('tab-replay')", "Replay"),
        ("5", "switch_tab('tab-operator')", "Operator"),
        ("r", "refresh_tab", "Refresh"),
        ("d", "toggle_dark", "Theme"),
        ("q", "request_quit", "Quit"),
    ]

    def compose(self) -> ComposeResult:
        # ── Header bar ──
        with Horizontal(id="header-bar"):
            yield Static("[bold #e8af34]GKR TRADING[/]", id="header-logo")
            yield Static("No session selected", id="header-session-info")
            yield Static("[#797876]--:-- ET[/]", id="header-right")

        # ── Tabbed content ──
        with TabbedContent(id="main-tabs"):
            # Tab 1: Sessions
            with TabPane("Sessions", id="tab-sessions"):
                with Horizontal(classes="split-h"):
                    with Vertical(classes="left-30 panel"):
                        yield SessionListPanel(id="session-list-panel")
                    with Vertical(classes="right-70 panel"):
                        yield SessionDetailPanel(id="session-detail-panel")

            # Tab 2: Positions
            with TabPane("Positions", id="tab-positions"):
                with Vertical():
                    with Vertical(classes="top-half panel"):
                        yield EquityPositionsTable(id="equity-pos")
                    with Vertical(classes="bottom-half panel"):
                        yield OptionsPositionsTable(id="options-pos")

            # Tab 3: Market Data
            with TabPane("Market", id="tab-market"):
                with Vertical():
                    with Vertical(classes="top-half panel"):
                        yield MarketDataTable(id="market-data")
                    with Vertical(classes="bottom-half panel", id="chart-panel"):
                        yield SparklinePanel(id="sparkline-panel")

            # Tab 4: Replay
            with TabPane("Replay", id="tab-replay"):
                with Vertical(classes="panel"):
                    yield Label(" Replay Validation", classes="section-header")
                    with Horizontal():
                        yield Button(
                            "Run Replay",
                            id="btn-replay",
                            classes="btn-accent",
                        )
                        yield Static("", id="replay-session-label")
                    yield Static("", id="replay-results")
                    yield Label(" Anomalies", classes="section-header")
                    yield DataTable(id="anomaly-table", cursor_type="none")

            # Tab 5: Operator
            with TabPane("Operator", id="tab-operator"):
                with Vertical(classes="panel scroll-panel"):
                    yield KillSwitchPanel(id="kill-switch-panel")
                    yield ReconciliationPanel(id="recon-panel")
                    yield AlertsPanel(id="alerts-panel")

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
            # Preserve market status if already set, just update time
            current = right.renderable
            if "OPEN" in str(current):
                right.update(f"[#6daa45]OPEN[/]  [#797876]{time_str}[/]")
            elif "CLOSED" in str(current):
                right.update(f"[#797876]CLOSED  {time_str}[/]")
            else:
                right.update(f"[#797876]{time_str}[/]")
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
        if session_id:
            sid_short = session_id[:16] + "…" if len(session_id) > 16 else session_id
            status_text = ""
            if status == "running":
                status_text = "[#4f98a3 bold]RUNNING[/]"
            elif status == "stopped":
                status_text = "[#797876]STOPPED[/]"
            elif status == "halted":
                status_text = "[#dd6974 bold]HALTED[/]"
            else:
                status_text = f"[#797876]{(status or '').upper()}[/]"

            info = self.query_one("#header-session-info", Static)
            info.update(f"[bold]{sid_short}[/]  {status_text}")

        if market_open is not None:
            right = self.query_one("#header-right", Static)
            if market_open:
                right.update("[#6daa45]OPEN[/]")
            else:
                right.update("[#797876]CLOSED[/]")

    # ── Session selection ──

    def on_session_selected(self, event: SessionSelected) -> None:
        self.app.set_active_session(event.session_id)

    # ── Ticker selection for sparkline ──

    def on_ticker_selected(self, event: TickerSelected) -> None:
        self.app.show_ticker_chart(event.ticker)

    # ── Kill switch ──

    def on_kill_switch_action(self, event: KillSwitchAction) -> None:
        self.app.handle_kill_switch(event.level)

    # ── Reconciliation ──

    def on_reconcile_action(self, event: ReconcileAction) -> None:
        self.app.handle_reconcile()

    # ── Replay ──

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-replay":
            self.app.handle_replay()
        elif event.button.id == "btn-new-session":
            self.app.handle_new_session()

    # ── Replay results ──

    def show_replay_results(
        self,
        session_id: str,
        cash: Decimal,
        position_count: int,
        event_count: int,
        anomalies: list,
    ) -> None:
        label = self.query_one("#replay-session-label", Static)
        label.update(f"  Session: {session_id[:20]}…")

        results = self.query_one("#replay-results", Static)
        anomaly_count = len(anomalies)
        if anomaly_count == 0:
            badge = "[#6daa45 bold]0 anomalies[/]"
        elif anomaly_count <= 5:
            badge = f"[#fdab43 bold]{anomaly_count} anomalies[/]"
        else:
            badge = f"[#dd6974 bold]{anomaly_count} anomalies[/]"

        results.update(
            f"[bold]Cash:[/] ${cash:,.2f}  |  "
            f"[bold]Positions:[/] {position_count}  |  "
            f"[bold]Events replayed:[/] {event_count}  |  "
            f"{badge}"
        )

        # Fill anomaly table
        table = self.query_one("#anomaly-table", DataTable)
        table.clear()
        for a in anomalies:
            code = getattr(a, "code", str(a)) if hasattr(a, "code") else "?"
            msg = getattr(a, "message", str(a)) if hasattr(a, "message") else str(a)
            idx = getattr(a, "event_index", "?") if hasattr(a, "event_index") else "?"
            color = "#dd6974" if "error" in str(code).lower() else "#fdab43"
            table.add_row(
                str(idx),
                str(getattr(a, "event_type", "—")),
                f"[{color}]{code}[/]",
                msg[:60],
            )

    def show_replay_error(self, error: str) -> None:
        results = self.query_one("#replay-results", Static)
        results.update(f"[#dd6974]Replay error: {error}[/]")
