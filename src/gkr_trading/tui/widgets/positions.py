"""Equity + options position tables, live positions, and account summary.

Contains:
  - EquityPositionsTable   (legacy DB-based equity positions)
  - OptionsPositionsTable  (legacy DB-based options positions)
  - LivePositionsTable     (NEW: live Alpaca API positions)
  - AccountSummaryBar      (NEW: portfolio/cash/P&L summary bar)
"""
from __future__ import annotations

from typing import Optional

from textual.app import ComposeResult
from textual.message import Message
from textual.widget import Widget
from textual.widgets import DataTable, Label, Static

from gkr_trading.tui.widgets.market_table import TickerSelected


def _fmt_cents(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    return f"${cents / 100:,.2f}"


def _fmt_pnl(cents: int) -> str:
    if cents > 0:
        return f"[#00e676 bold]+${cents / 100:,.2f}[/]"
    elif cents < 0:
        return f"[#ff4444 bold]-${abs(cents) / 100:,.2f}[/]"
    return "[#444444]$0.00[/]"


def _fmt_pct(pct: float) -> str:
    if pct > 0:
        return f"[#00e676 bold]+{pct:.2f}%[/]"
    elif pct < 0:
        return f"[#ff4444 bold]{pct:.2f}%[/]"
    return "[#444444]0.00%[/]"


# ── Live Positions Table (from Alpaca API) ────────────────────────────────

class LivePositionsTable(Widget):
    """Shows positions from AlpacaPositionsWorker (live Alpaca API data)."""

    def compose(self) -> ComposeResult:
        yield DataTable(id="live-pos-table", cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one("#live-pos-table", DataTable)
        table.add_columns(
            "TICKER", "QTY", "AVG ENTRY", "LAST", "P&L", "P&L%", "SIDE", "MKT VALUE"
        )
        table.zebra_stripes = True

    def update_positions(self, positions: list) -> None:
        """Replace all rows with live position data."""
        table = self.query_one("#live-pos-table", DataTable)
        table.clear()

        if not positions:
            return

        for pos in positions:
            ticker = pos.ticker
            qty = pos.qty
            avg_entry = _fmt_cents(pos.avg_entry_cents)
            last = _fmt_cents(pos.last_cents)
            pnl = _fmt_pnl(pos.unrealized_pnl_cents)
            pnl_pct = _fmt_pct(pos.unrealized_pnl_pct)
            side = pos.side.upper()
            mkt_val = _fmt_cents(pos.market_value_cents)

            table.add_row(
                ticker,
                str(int(qty)) if qty == int(qty) else str(qty),
                avg_entry,
                last,
                pnl,
                pnl_pct,
                side,
                mkt_val,
                key=ticker,
            )

    def update_prices(self, prices: dict[str, int]) -> None:
        """Update Last column without full reload (for market data updates)."""
        # For simplicity, just trigger full update via app — this is called rarely
        pass

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key and event.row_key.value:
            self.post_message(TickerSelected(event.row_key.value))


# ── Account Summary Bar ──────────────────────────────────────────────────

class AccountSummaryBar(Widget):
    """Single-row summary: Portfolio | Unrealized P&L | Cash | Buying Power."""

    def compose(self) -> ComposeResult:
        yield Static(
            "[#444444]Waiting for account data...[/]",
            id="account-summary-text",
            classes="account-summary-bar",
        )

    def update_summary(self, account) -> None:
        """Update with LiveAccountSummary data."""
        text_w = self.query_one("#account-summary-text", Static)
        if account is None:
            text_w.update("[#444444]No account data available[/]")
            return

        portfolio = _fmt_cents(account.portfolio_value_cents)
        pnl = _fmt_pnl(account.unrealized_pnl_cents)
        cash = _fmt_cents(account.cash_cents)
        bp = _fmt_cents(account.buying_power_cents)

        text_w.update(
            f"[#666666]Portfolio:[/] [bold]{portfolio}[/]  "
            f"[#666666]P&L:[/] {pnl}  "
            f"[#666666]Cash:[/] [bold]{cash}[/]  "
            f"[#666666]Buying Power:[/] {bp}"
        )


# ── Legacy DB-based tables (kept for audit/replay) ───────────────────────

class EquityPositionsTable(Static):
    """Top panel: equity positions DataTable."""

    def compose(self) -> ComposeResult:
        yield Label("EQUITY POSITIONS", classes="section-label")
        yield DataTable(id="equity-pos-table", cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one("#equity-pos-table", DataTable)
        table.add_columns(
            "Ticker", "Qty", "Avg Cost", "Last Price", "MtM Value",
            "Unrealized PnL", "Realized PnL", "Status",
        )
        table.zebra_stripes = True

    def update_positions(
        self,
        positions: list[dict],
        market_prices: dict[str, int] | None = None,
    ) -> None:
        table = self.query_one("#equity-pos-table", DataTable)
        table.clear()

        if not positions:
            return

        prices = market_prices or {}

        for pos in positions:
            ticker = pos.get("ticker", "?")
            qty = pos.get("signed_qty", 0)
            cost_basis = pos.get("cost_basis_cents", 0)
            realized = pos.get("realized_pnl_cents", 0)
            status = pos.get("status", "open")

            avg_cost = cost_basis // qty if qty != 0 else 0
            last_price = prices.get(ticker)
            mtm = last_price * qty if last_price is not None and qty != 0 else None
            unrealized = (last_price - avg_cost) * qty if last_price is not None and qty != 0 else 0

            status_display = status
            if status == "open":
                status_display = "[#00c9a7]OPEN[/]"
            elif status == "closed":
                status_display = "[#444444]CLOSED[/]"

            table.add_row(
                ticker,
                str(qty),
                _fmt_cents(avg_cost),
                _fmt_cents(last_price),
                _fmt_cents(mtm),
                _fmt_pnl(unrealized),
                _fmt_pnl(realized),
                status_display,
                key=ticker,
            )


class OptionsPositionsTable(Static):
    """Bottom panel: options positions DataTable."""

    def compose(self) -> ComposeResult:
        yield Label("OPTIONS POSITIONS", classes="section-label")
        yield DataTable(id="options-pos-table", cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one("#options-pos-table", DataTable)
        table.add_columns(
            "OCC Symbol", "Long", "Short", "Premium Paid",
            "Premium Rcvd", "Realized PnL", "Status",
        )
        table.zebra_stripes = True

    def update_positions(self, positions: list[dict]) -> None:
        table = self.query_one("#options-pos-table", DataTable)
        table.clear()

        if not positions:
            return

        for pos in positions:
            occ = pos.get("occ_symbol", "?")
            long_c = pos.get("long_contracts", 0)
            short_c = pos.get("short_contracts", 0)
            premium_paid = pos.get("long_premium_paid_cents", 0)
            premium_rcvd = pos.get("short_premium_received_cents", 0)
            realized = pos.get("realized_pnl_cents", 0)
            status = pos.get("status", "open")

            status_display = status
            if status == "open":
                status_display = "[#00c9a7]OPEN[/]"
            elif status == "closed":
                status_display = "[#444444]CLOSED[/]"
            elif status == "expired":
                status_display = "[#444444]EXPIRED[/]"

            table.add_row(
                occ,
                str(long_c),
                str(short_c),
                _fmt_cents(premium_paid),
                _fmt_cents(premium_rcvd),
                _fmt_pnl(realized),
                status_display,
                key=occ,
            )
