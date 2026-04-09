"""Operator panel — kill switch buttons, reconciliation, alerts."""
from __future__ import annotations

import subprocess
import sys
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Button, DataTable, Label, Static


class KillSwitchAction(Message):
    """Posted when operator clicks a kill switch button."""

    def __init__(self, level: str) -> None:
        super().__init__()
        self.level = level


class ReconcileAction(Message):
    """Posted when operator clicks Reconcile Now."""
    pass


class KillSwitchPanel(Widget):
    """Kill switch section with three toggle buttons."""

    def compose(self) -> ComposeResult:
        yield Label("KILL SWITCH", classes="section-label")
        yield Static("", id="ks-current-level")
        with Horizontal(id="ks-buttons"):
            yield Button("NONE", id="ks-none", classes="ks-none")
            yield Button("CLOSE ONLY", id="ks-close-only", classes="ks-close-only")
            yield Button("FULL HALT", id="ks-full-halt", classes="ks-full-halt")

    def update_level(self, level: str) -> None:
        label = self.query_one("#ks-current-level", Static)
        if level == "none":
            label.update("[#00e676 bold]Kill Switch: NONE (all trading enabled)[/]")
            self._set_active("ks-none")
        elif level == "close_only":
            label.update("[#ffb300 bold]Kill Switch: CLOSE ONLY (no new opens)[/]")
            self._set_active("ks-close-only")
        elif level == "full_halt":
            label.update("[#ff4444 bold]Kill Switch: FULL HALT (all blocked)[/]")
            self._set_active("ks-full-halt")
        else:
            label.update(f"[#444444]Kill Switch: {level}[/]")

    def _set_active(self, active_id: str) -> None:
        for btn_id in ("ks-none", "ks-close-only", "ks-full-halt"):
            btn = self.query_one(f"#{btn_id}", Button)
            btn.remove_class("ks-active")
        try:
            self.query_one(f"#{active_id}", Button).add_class("ks-active")
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        mapping = {
            "ks-none": "none",
            "ks-close-only": "close_only",
            "ks-full-halt": "full_halt",
        }
        level = mapping.get(event.button.id or "", "")
        if level:
            self.post_message(KillSwitchAction(level))


class ReconciliationPanel(Widget):
    """Reconciliation section with trigger button and results table."""

    def compose(self) -> ComposeResult:
        yield Label("RECONCILIATION", classes="section-label")
        yield Button("Reconcile Now", id="btn-reconcile", classes="btn-accent")
        yield Static("", id="recon-status")
        yield DataTable(id="recon-table", cursor_type="none")

    def on_mount(self) -> None:
        table = self.query_one("#recon-table", DataTable)
        table.add_columns("Field", "Local Value", "Venue Value", "Severity")
        table.zebra_stripes = True

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-reconcile":
            self.post_message(ReconcileAction())

    def show_results(self, status: str, breaks: list[dict]) -> None:
        status_widget = self.query_one("#recon-status", Static)
        if status == "clean":
            status_widget.update("[#00e676 bold]CLEAN[/]")
        else:
            status_widget.update("[#ff4444 bold]BREAKS DETECTED[/]")

        table = self.query_one("#recon-table", DataTable)
        table.clear()
        for b in breaks:
            severity = b.get("severity", "?")
            if severity == "blocking":
                sev_display = "[#ff4444 bold]BLOCKING[/]"
            elif severity == "warning":
                sev_display = "[#ffb300]WARNING[/]"
            else:
                sev_display = severity
            table.add_row(
                b.get("field", "?"),
                b.get("local_value", "?"),
                b.get("venue_value", "?"),
                sev_display,
            )

    def show_error(self, error: str) -> None:
        status_widget = self.query_one("#recon-status", Static)
        status_widget.update(f"[#ff4444]Error: {error}[/]")


class AlertsPanel(Widget):
    """Alerts section — notable events from the stream."""

    def compose(self) -> ComposeResult:
        yield Label("ALERTS", classes="section-label")
        yield DataTable(id="alerts-table", cursor_type="none")

    def on_mount(self) -> None:
        table = self.query_one("#alerts-table", DataTable)
        table.add_columns("Time", "Type", "Details")
        table.zebra_stripes = True

    def update_alerts(self, events: list) -> None:
        """Filter and display alert-worthy events."""
        table = self.query_one("#alerts-table", DataTable)
        table.clear()

        alert_types = {
            "assignment_received",
            "reconciliation_completed",
            "operator_command",
            "risk_rejected",
            "exercise_processed",
            "expiration_processed",
        }

        alerts = [e for e in events if e.event_type in alert_types]
        # Show most recent first, max 20
        for ev in reversed(alerts[-20:]):
            ts = ev.occurred_at[:19] if len(ev.occurred_at) > 19 else ev.occurred_at
            etype = ev.event_type.replace("_", " ").title()

            color = "#ffb300"
            if "reject" in ev.event_type:
                color = "#ff4444"
            elif "assignment" in ev.event_type:
                color = "#ff4444"

            table.add_row(
                ts,
                f"[{color}]{etype}[/]",
                ev.payload_summary[:50],
            )
