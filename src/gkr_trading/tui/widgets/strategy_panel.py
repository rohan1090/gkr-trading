"""Strategy panels — toggle, allocation, and full control views.

Contains:
  - StrategyAllocationPanel (compact sidebar for Positions tab)
  - StrategyControlPanel    (full card view for Strategies tab)
  - Messages: StrategyToggled, StrategyAllocationChanged,
              StrategyStartRequested, StrategyPauseRequested
"""
from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Button, Label, Static


# ── Available strategies (TODO: make dynamic from config/registry) ────────

AVAILABLE_STRATEGIES = [
    "equity_momentum",
    "mean_reversion",
    "options_volatility",
]

DEFAULT_STRATEGY_STATE = {
    "active": False,
    "alloc_pct": 0,
    "status": "inactive",   # "running" | "inactive" | "halted"
    "trade_count": 0,
    "pnl_cents": 0,
    "session_id": None,
}


# ── Messages ──────────────────────────────────────────────────────────────

class StrategyToggled(Message):
    """Posted when a strategy is toggled on/off."""
    def __init__(self, strategy_name: str, active: bool) -> None:
        super().__init__()
        self.strategy_name = strategy_name
        self.active = active


class StrategyAllocationChanged(Message):
    """Posted when allocation % changes."""
    def __init__(self, strategy_name: str, new_pct: int) -> None:
        super().__init__()
        self.strategy_name = strategy_name
        self.new_pct = new_pct


class StrategyStartRequested(Message):
    """Posted when user clicks Start on a strategy."""
    def __init__(self, strategy_name: str) -> None:
        super().__init__()
        self.strategy_name = strategy_name


class StrategyPauseRequested(Message):
    """Posted when user clicks Pause on a strategy."""
    def __init__(self, strategy_name: str) -> None:
        super().__init__()
        self.strategy_name = strategy_name


def _status_badge(status: str) -> str:
    """Render a colored status badge."""
    if status == "running":
        return "[#00e676 bold]● RUNNING[/]"
    elif status == "halted":
        return "[#ff4444 bold]⊗ HALTED[/]"
    return "[#444444]○ INACTIVE[/]"


def _fmt_pnl(cents: int) -> str:
    if cents > 0:
        return f"[#00e676 bold]+${cents / 100:,.2f}[/]"
    elif cents < 0:
        return f"[#ff4444 bold]-${abs(cents) / 100:,.2f}[/]"
    return "[#444444]$0.00[/]"


# ── StrategyAllocationPanel (compact, for Positions tab sidebar) ──────────

class StrategyAllocationPanel(Widget):
    """Compact strategy toggles with allocation percentage.

    Each row: [●/○] strategy_name  15%  [–] [+]
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._states: dict[str, dict] = {}

    def compose(self) -> ComposeResult:
        for name in AVAILABLE_STRATEGIES:
            with Horizontal(classes="alloc-row", id=f"alloc-{name}"):
                yield Button("○", id=f"alloc-toggle-{name}", classes="alloc-toggle")
                yield Static(f" {name}", classes="alloc-name")
                yield Static(" 0%", id=f"alloc-pct-{name}", classes="alloc-pct")
                yield Button("–", id=f"alloc-dec-{name}", classes="alloc-btn")
                yield Button("+", id=f"alloc-inc-{name}", classes="alloc-btn")
        yield Static("", id="alloc-summary", classes="alloc-summary-line")

    def update_strategy_states(self, states: dict[str, dict]) -> None:
        self._states = states
        total_pct = 0
        for name in AVAILABLE_STRATEGIES:
            st = states.get(name, DEFAULT_STRATEGY_STATE)
            active = st.get("active", False)
            pct = st.get("alloc_pct", 0)
            total_pct += pct

            try:
                toggle = self.query_one(f"#alloc-toggle-{name}", Button)
                toggle.label = "●" if active else "○"
            except Exception:
                pass
            try:
                pct_label = self.query_one(f"#alloc-pct-{name}", Static)
                pct_label.update(f" {pct}%")
            except Exception:
                pass

        try:
            summary = self.query_one("#alloc-summary", Static)
            cash_pct = 100 - total_pct
            summary.update(f"[#666666]Allocated: {total_pct}%  Cash: {cash_pct}%[/]")
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        btn_id = event.button.id or ""

        for name in AVAILABLE_STRATEGIES:
            if btn_id == f"alloc-toggle-{name}":
                st = self._states.get(name, dict(DEFAULT_STRATEGY_STATE))
                new_active = not st.get("active", False)
                self.post_message(StrategyToggled(name, new_active))
                return
            elif btn_id == f"alloc-inc-{name}":
                st = self._states.get(name, dict(DEFAULT_STRATEGY_STATE))
                new_pct = min(100, st.get("alloc_pct", 0) + 5)
                self.post_message(StrategyAllocationChanged(name, new_pct))
                return
            elif btn_id == f"alloc-dec-{name}":
                st = self._states.get(name, dict(DEFAULT_STRATEGY_STATE))
                new_pct = max(0, st.get("alloc_pct", 0) - 5)
                self.post_message(StrategyAllocationChanged(name, new_pct))
                return


# ── StrategyControlPanel (full view, for Strategies tab) ──────────────────

class StrategyControlPanel(Widget):
    """Full strategy control cards stacked vertically.

    Each card (height ~5):
      Row 1: ● STRATEGY_NAME    Status badge    P&L today
      Row 2: Alloc: 15%   Trades: 3
      Row 3: [Toggle ON/OFF]  [Start]  [Pause]
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._states: dict[str, dict] = {}

    def compose(self) -> ComposeResult:
        for name in AVAILABLE_STRATEGIES:
            with Vertical(classes="strategy-card", id=f"scard-{name}"):
                # Row 1: name + status + pnl
                with Horizontal(classes="scard-row"):
                    yield Static(
                        f"[bold #e0e0e0]{name}[/]",
                        id=f"scard-name-{name}",
                        classes="scard-name",
                    )
                    yield Static(
                        _status_badge("inactive"),
                        id=f"scard-status-{name}",
                        classes="scard-status",
                    )
                    yield Static(
                        "[#444444]—[/]",
                        id=f"scard-pnl-{name}",
                        classes="scard-pnl",
                    )
                # Row 2: allocation + trades
                with Horizontal(classes="scard-row"):
                    yield Static(
                        "[#666666]Alloc: 0%   Trades: 0[/]",
                        id=f"scard-info-{name}",
                        classes="scard-info",
                    )
                # Row 3: buttons
                with Horizontal(classes="scard-row"):
                    yield Button(
                        "Enable", id=f"scard-toggle-{name}", classes="btn-accent"
                    )
                    yield Button(
                        "Start", id=f"scard-start-{name}"
                    )
                    yield Button(
                        "Pause", id=f"scard-pause-{name}"
                    )
        # Summary row
        with Horizontal(classes="scard-summary"):
            yield Static("", id="scard-total-summary")
            yield Button("START ALL ACTIVE", id="scard-start-all", classes="btn-accent")
            yield Button("PAUSE ALL", id="scard-pause-all", classes="btn-warning")

    def update_strategy_states(self, states: dict[str, dict]) -> None:
        self._states = states
        total_pct = 0
        for name in AVAILABLE_STRATEGIES:
            st = states.get(name, DEFAULT_STRATEGY_STATE)
            active = st.get("active", False)
            status = st.get("status", "inactive")
            pct = st.get("alloc_pct", 0)
            trades = st.get("trade_count", 0)
            pnl = st.get("pnl_cents", 0)
            total_pct += pct

            # Card class
            try:
                card = self.query_one(f"#scard-{name}")
                card.remove_class("-active")
                card.remove_class("-halted")
                if status == "running":
                    card.add_class("-active")
                elif status == "halted":
                    card.add_class("-halted")
            except Exception:
                pass

            # Status badge
            try:
                sb = self.query_one(f"#scard-status-{name}", Static)
                sb.update(_status_badge(status))
            except Exception:
                pass

            # P&L
            try:
                pnl_w = self.query_one(f"#scard-pnl-{name}", Static)
                pnl_w.update(_fmt_pnl(pnl) if pnl != 0 else "[#444444]—[/]")
            except Exception:
                pass

            # Info line
            try:
                info_w = self.query_one(f"#scard-info-{name}", Static)
                info_w.update(f"[#666666]Alloc: {pct}%   Trades: {trades}[/]")
            except Exception:
                pass

            # Toggle button label
            try:
                toggle = self.query_one(f"#scard-toggle-{name}", Button)
                toggle.label = "Disable" if active else "Enable"
            except Exception:
                pass

        # Summary
        try:
            summary = self.query_one("#scard-total-summary", Static)
            cash_pct = 100 - total_pct
            summary.update(
                f"[#666666]Total allocated: {total_pct}%   Cash reserve: {cash_pct}%[/]"
            )
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        btn_id = event.button.id or ""

        if btn_id == "scard-start-all":
            for name in AVAILABLE_STRATEGIES:
                st = self._states.get(name, DEFAULT_STRATEGY_STATE)
                if st.get("active", False) and st.get("status") != "running":
                    self.post_message(StrategyStartRequested(name))
            return

        if btn_id == "scard-pause-all":
            for name in AVAILABLE_STRATEGIES:
                st = self._states.get(name, DEFAULT_STRATEGY_STATE)
                if st.get("status") == "running":
                    self.post_message(StrategyPauseRequested(name))
            return

        for name in AVAILABLE_STRATEGIES:
            if btn_id == f"scard-toggle-{name}":
                st = self._states.get(name, dict(DEFAULT_STRATEGY_STATE))
                new_active = not st.get("active", False)
                self.post_message(StrategyToggled(name, new_active))
                return
            elif btn_id == f"scard-start-{name}":
                self.post_message(StrategyStartRequested(name))
                return
            elif btn_id == f"scard-pause-{name}":
                self.post_message(StrategyPauseRequested(name))
                return
