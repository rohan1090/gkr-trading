"""Scrollable live event feed — footer widget."""
from __future__ import annotations

from textual.app import ComposeResult
from textual.widgets import RichLog, Static, Label

from gkr_trading.tui.workers.db_watcher import EventSummary

# Color map for event types
_EVENT_COLORS: dict[str, str] = {
    "session_started": "#4f98a3",
    "session_stopped": "#797876",
    "trade_intent_created": "#e8af34",
    "risk_approved": "#6daa45",
    "risk_rejected": "#dd6974",
    "order_submitted": "#4f98a3",
    "order_submission_attempted": "#4f98a3",
    "fill_received": "#6daa45",
    "pending_order_registered": "#797876",
    "reconciliation_completed": "#fdab43",
    "operator_command": "#fdab43",
    "assignment_received": "#dd6974",
    "exercise_processed": "#fdab43",
    "expiration_processed": "#797876",
    "market_data_received": "#797876",
}


def _format_event(ev: EventSummary) -> str:
    """Format a single event for RichLog display."""
    color = _EVENT_COLORS.get(ev.event_type, "#cdccca")
    ts = ev.occurred_at
    if len(ts) > 19:
        ts = ts[:19]  # Trim to YYYY-MM-DDTHH:MM:SS
    etype = ev.event_type.replace("_", " ").title()
    summary = ev.payload_summary
    if len(summary) > 60:
        summary = summary[:57] + "…"
    return f"[#797876]{ts}[/] [{color}]{etype:30s}[/] {summary}"


class EventLogFooter(Static):
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
