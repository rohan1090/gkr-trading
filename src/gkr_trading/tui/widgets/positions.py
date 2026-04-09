"""Equity + options position tables."""
from __future__ import annotations

from typing import Optional

from textual.app import ComposeResult
from textual.widgets import DataTable, Label, Static


def _fmt_cents(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    return f"${cents / 100:,.2f}"


def _fmt_pnl(cents: int) -> str:
    if cents > 0:
        return f"[#6daa45]+${cents / 100:,.2f}[/]"
    elif cents < 0:
        return f"[#dd6974]-${abs(cents) / 100:,.2f}[/]"
    return f"[#797876]$0.00[/]"


class EquityPositionsTable(Static):
    """Top panel: equity positions DataTable."""

    def compose(self) -> ComposeResult:
        yield Label(" Equity Positions", classes="section-header")
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
                status_display = "[#4f98a3]OPEN[/]"
            elif status == "closed":
                status_display = "[#797876]CLOSED[/]"

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
        yield Label(" Options Positions", classes="section-header")
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
                status_display = "[#4f98a3]OPEN[/]"
            elif status == "closed":
                status_display = "[#797876]CLOSED[/]"
            elif status == "expired":
                status_display = "[#797876]EXPIRED[/]"

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
