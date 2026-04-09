"""Live market data table + sparkline chart panel."""
from __future__ import annotations

import io
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import DataTable, Label, Static


class TickerSelected(Message):
    """Posted when user clicks a ticker row."""

    def __init__(self, ticker: str) -> None:
        super().__init__()
        self.ticker = ticker


def _fmt_price(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    return f"${cents / 100:,.2f}"


def _fmt_volume(vol: Optional[int]) -> str:
    if vol is None:
        return "—"
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.1f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.1f}K"
    return str(vol)


class MarketDataTable(Static):
    """Top panel: live ticker prices in a DataTable."""

    def compose(self) -> ComposeResult:
        yield Label(" Market Data", classes="section-header")
        yield DataTable(id="market-table", cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one("#market-table", DataTable)
        table.add_columns(
            "Ticker", "Last", "Bid", "Ask", "Open", "High", "Low", "Volume", "Chg", "Chg%"
        )
        table.zebra_stripes = True

    def update_data(self, snapshots: list) -> None:
        """Update table with new MarketDataSnapshot list."""
        table = self.query_one("#market-table", DataTable)
        table.clear()

        for snap in snapshots:
            ticker = snap.ticker
            last = snap.last_cents
            open_p = snap.open_cents

            # Compute change
            chg_str = "—"
            chg_pct_str = "—"
            if last is not None and open_p is not None and open_p > 0:
                chg = last - open_p
                chg_pct = (chg / open_p) * 100
                sign = "+" if chg >= 0 else ""
                color = "#6daa45" if chg >= 0 else "#dd6974"
                chg_str = f"[{color}]{sign}${chg / 100:.2f}[/]"
                chg_pct_str = f"[{color}]{sign}{chg_pct:.2f}%[/]"

            table.add_row(
                ticker,
                _fmt_price(last),
                _fmt_price(snap.bid_cents),
                _fmt_price(snap.ask_cents),
                _fmt_price(open_p),
                _fmt_price(snap.high_cents),
                _fmt_price(snap.low_cents),
                _fmt_volume(snap.volume),
                chg_str,
                chg_pct_str,
                key=ticker,
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key and event.row_key.value:
            self.post_message(TickerSelected(event.row_key.value))


class SparklinePanel(Static):
    """Bottom panel: ASCII sparkline chart for selected ticker."""

    def compose(self) -> ComposeResult:
        yield Label(" Price Chart", classes="section-header")
        yield Static(
            "[#797876]Click a ticker above to show price chart[/]",
            id="sparkline-content",
        )

    def update_chart(self, ticker: str, prices: list[int]) -> None:
        """Render an ASCII sparkline using plotext."""
        content = self.query_one("#sparkline-content", Static)

        if not prices or len(prices) < 2:
            content.update(f"[#797876]{ticker}: Not enough data for chart[/]")
            return

        try:
            import plotext as plt

            plt.clear_figure()
            plt.theme("dark")
            plt.plot_size(80, 15)

            # Convert cents to dollars
            dollars = [p / 100 for p in prices]
            plt.plot(dollars, marker="braille")
            plt.title(f"{ticker}  ${dollars[-1]:,.2f}")
            plt.xlabel("Time (15s intervals)")
            plt.ylabel("Price ($)")

            # Capture to string
            buf = io.StringIO()
            plt.savefig(buf)
            chart_text = buf.getvalue()
            content.update(chart_text)
        except Exception as exc:
            content.update(f"[#dd6974]Chart error: {exc}[/]")

    def show_no_credentials(self) -> None:
        content = self.query_one("#sparkline-content", Static)
        content.update(
            "[#fdab43]No Alpaca credentials — market data unavailable[/]"
        )

    def show_market_closed(self) -> None:
        content = self.query_one("#sparkline-content", Static)
        content.update(
            "[#797876]Market closed — showing last available data[/]"
        )
