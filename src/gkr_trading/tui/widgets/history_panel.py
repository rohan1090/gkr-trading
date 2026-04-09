"""Trading history panel — sessions grouped by date, no UUIDs visible."""
from __future__ import annotations

from textual.app import ComposeResult
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Button, DataTable, Label, Static

from gkr_trading.tui.widgets.session_list import SessionSelected


class ReplayRequested(Message):
    """Posted when user clicks [REPLAY] for a session."""
    def __init__(self, session_id: str) -> None:
        super().__init__()
        self.session_id = session_id


def _status_badge(status: str) -> str:
    if status == "running":
        return "[#00e676 bold]● RUNNING[/]"
    elif status == "halted":
        return "[#ff4444 bold]⊗ HALTED[/]"
    elif status == "stopped":
        return "[#444444]○ STOPPED[/]"
    return f"[#444444]○ {status.upper()}[/]"


def _fmt_pnl(cents: int) -> str:
    if cents > 0:
        return f"[#00e676 bold]+${cents / 100:,.2f}[/]"
    elif cents < 0:
        return f"[#ff4444 bold]-${abs(cents) / 100:,.2f}[/]"
    return "[#444444]—[/]"


class TradingHistoryPanel(Widget):
    """Shows sessions grouped by date, most recent first.

    No UUIDs visible — dates and strategy names only.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._sessions: list[dict] = []

    def compose(self) -> ComposeResult:
        yield DataTable(id="history-table", cursor_type="row")

    def on_mount(self) -> None:
        table = self.query_one("#history-table", DataTable)
        table.add_columns("DATE", "STRATEGY", "EVENTS", "P&L", "STATUS")
        table.zebra_stripes = True

    def update_history(self, sessions: list[dict]) -> None:
        """Update the history table with enriched session data.

        Expects sessions from DBWatcher.get_sessions_with_dates() which
        include date_str, is_today, strategy, status, event_count.
        """
        self._sessions = sessions
        table = self.query_one("#history-table", DataTable)
        table.clear()

        for s in sessions:
            sid = s.get("session_id", "")
            date_str = s.get("date_str", "Unknown date")
            is_today = s.get("is_today", False)
            strategy = s.get("strategy", "—") or "—"
            status = s.get("status", "unknown")
            event_count = s.get("event_count", 0)
            pnl_cents = s.get("pnl_cents", 0)

            # Format date column
            if is_today:
                date_display = f"[bold]{date_str}[/] [#00c9a7](Today)[/]"
            else:
                date_display = f"[#888888]{date_str}[/]"

            table.add_row(
                date_display,
                strategy,
                str(event_count),
                _fmt_pnl(pnl_cents),
                _status_badge(status),
                key=sid,
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Set the selected session as active."""
        if event.row_key and event.row_key.value:
            self.post_message(SessionSelected(event.row_key.value))
