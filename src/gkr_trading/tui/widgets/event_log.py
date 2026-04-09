"""Scrollable live event feed — footer widget."""
from __future__ import annotations

from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import RichLog, Static, Label

from gkr_trading.tui.workers.db_watcher import EventSummary

# Color map for event types (updated for Bloomberg terminal palette)
_EVENT_COLORS: dict[str, str] = {
    "session_started": "#00c9a7",
    "session_stopped": "#444444",
    "trade_intent_created": "#ffb300",
    "risk_approved": "#00e676",
    "risk_rejected": "#ff4444",
    "order_submitted": "#00c9a7",
    "order_submission_attempted": "#00c9a7",
    "fill_received": "#00e676",
    "pending_order_registered": "#666666",
    "reconciliation_completed": "#ffb300",
    "operator_command": "#ffb300",
    "assignment_received": "#ff4444",
    "exercise_processed": "#ffb300",
    "expiration_processed": "#444444",
    "market_data_received": "#444444",
}


def _format_event(ev: EventSummary) -> str:
    """Format a single event for RichLog display."""
    color = _EVENT_COLORS.get(ev.event_type, "#e0e0e0")
    ts = ev.occurred_at
    if len(ts) > 19:
        ts = ts[11:19]  # Extract just HH:MM:SS from ISO timestamp
    elif len(ts) > 8:
        ts = ts[:8]
    etype = ev.event_type.replace("_", " ").title()
    summary = ev.payload_summary
    if len(summary) > 60:
        summary = summary[:57] + "..."
    return f"[#444444]{ts}[/] [{color}]{etype:30s}[/] {summary}"


class EventLogFooter(Widget):
    """Footer event log — shows last N events with auto-scroll."""

    MAX_LINES = 200

    def compose(self) -> ComposeResult:
        yield RichLog(id="event-richlog", wrap=True, markup=True, max_lines=self.MAX_LINES)

    def append_events(self, events: list[EventSummary]) -> None:
        log = self.query_one("#event-richlog", RichLog)
        for ev in events:
            log.write(_format_event(ev))

    def clear_log(self) -> None:
        log = self.query_one("#event-richlog", RichLog)
        log.clear()

    def load_events(self, events: list[EventSummary]) -> None:
        """Replace log with a full list of events (for session switch)."""
        log = self.query_one("#event-richlog", RichLog)
        log.clear()
        # Only show last 50 events to avoid startup lag
        recent = events[-50:] if len(events) > 50 else events
        for ev in recent:
            log.write(_format_event(ev))
