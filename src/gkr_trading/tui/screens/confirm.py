"""Confirmation modal screen — used for kill switch and destructive actions."""
from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static


class ConfirmScreen(ModalScreen[bool]):
    """Modal confirmation dialog.

    Returns True if confirmed, False if cancelled.
    """

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]

    def __init__(self, title: str, message: str, confirm_label: str = "Confirm") -> None:
        super().__init__()
        self._title = title
        self._message = message
        self._confirm_label = confirm_label

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Label(self._title, id="confirm-title")
            yield Static(self._message, id="confirm-message")
            with Horizontal(id="confirm-buttons"):
                yield Button("Cancel", id="confirm-cancel", variant="default")
                yield Button(
                    self._confirm_label, id="confirm-ok", classes="btn-danger"
                )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm-ok":
            self.dismiss(True)
        else:
            self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)
