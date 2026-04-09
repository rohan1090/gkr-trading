"""Sessions panel — clickable DataTable of all sessions."""
from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Button, DataTable, Label, Static


class SessionSelected(Message):
    """Posted when user clicks a session row."""

    def __init__(self, session_id: str) -> None:
        super().__init__()
        self.session_id = session_id


class SessionListPanel(Widget):
    """Left panel: scrollable list of sessions."""

    def compose(self) -> ComposeResult:
        yield Label(" Sessions", classes="section-header")
        yield DataTable(id="session-table", cursor_type="row")
        yield Button("New Session", id="btn-new-session", classes="btn-accent")

    def on_mount(self) -> None:
        table = self.query_one("#session-table", DataTable)
        table.add_columns("St", "Session ID", "Strategy", "Events", "Status")
        table.zebra_stripes = True

    def update_sessions(self, sessions: list[dict]) -> None:
        table = self.query_one("#session-table", DataTable)
        table.clear()
        for s in sessions:
            sid = s.get("session_id", "")
            status = s.get("status", "unknown")
            strategy = s.get("strategy", "")
            event_count = s.get("event_count", 0)
            stop_reason = s.get("stop_reason", "")

            if status == "running":
                badge = "[#4f98a3]●[/]"
            elif status == "stopped":
                badge = "[#797876]●[/]"
            elif status == "halted":
                badge = "[#dd6974]●[/]"
            else:
                badge = "[#797876]○[/]"

            display_id = sid[:12] + "…" if len(sid) > 12 else sid
            display_status = stop_reason if stop_reason else status

            table.add_row(
                badge,
                display_id,
                strategy or "—",
                str(event_count),
                display_status,
                key=sid,
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key and event.row_key.value:
            self.post_message(SessionSelected(event.row_key.value))


class SessionDetailPanel(Widget):
    """Right panel: active session stats."""

    def compose(self) -> ComposeResult:
        yield Label(" Session Details", classes="section-header")
        yield Static("No session selected", id="session-detail-content")

    def update_detail(
        self, session_id: str, sessions: list[dict], events: list | None = None
    ) -> None:
        content = self.query_one("#session-detail-content", Static)
        info = None
        for s in sessions:
            if s.get("session_id") == session_id:
                info = s
                break

        if not info:
            content.update(f"Session: {session_id[:16]}…\nNo data available.")
            return

        status = info.get("status", "unknown")
        strategy = info.get("strategy", "—")
        event_count = info.get("event_count", 0)
        stop_reason = info.get("stop_reason", "")

        if status == "running":
            status_display = "[#4f98a3 bold]RUNNING[/]"
        elif status == "stopped":
            status_display = "[#797876]STOPPED[/]"
        elif status == "halted":
            status_display = "[#dd6974 bold]HALTED[/]"
        else:
            status_display = f"[#797876]{status.upper()}[/]"

        lines = [
            f"[bold]Session:[/] {session_id[:24]}…" if len(session_id) > 24 else f"[bold]Session:[/] {session_id}",
            f"[bold]Status:[/]   {status_display}",
            f"[bold]Strategy:[/] {strategy or '—'}",
            f"[bold]Events:[/]   {event_count}",
        ]
        if stop_reason:
            lines.append(f"[bold]Stop:[/]     {stop_reason}")

        # Count event types if we have events
        if events:
            intent_count = sum(1 for e in events if "intent" in e.event_type)
            fill_count = sum(1 for e in events if "fill" in e.event_type)
            order_count = sum(1 for e in events if "submitted" in e.event_type or "order" in e.event_type.lower())
            lines.append("")
            lines.append(f"[bold]Intents:[/]  {intent_count}")
            lines.append(f"[bold]Orders:[/]   {order_count}")
            lines.append(f"[bold]Fills:[/]    {fill_count}")

        content.update("\n".join(lines))
