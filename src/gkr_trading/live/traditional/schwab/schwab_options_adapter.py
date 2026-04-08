"""Schwab options adapter — STUB. Not implemented."""
from __future__ import annotations

from typing import List, Optional

from gkr_trading.core.options_lifecycle import AssignmentEvent, ExerciseEvent, ExpirationEvent
from gkr_trading.live.traditional.options.options_adapter_base import OptionsCapableAdapterMixin


class SchwabOptionsAdapter(OptionsCapableAdapterMixin):
    """STUB: Schwab options adapter. Not yet implemented."""

    def get_options_positions(self) -> List[dict]:
        raise NotImplementedError("SchwabOptionsAdapter is not yet implemented")

    def get_pending_assignments(self) -> List[str]:
        raise NotImplementedError("SchwabOptionsAdapter is not yet implemented")

    def get_expiring_today(self) -> List[str]:
        raise NotImplementedError("SchwabOptionsAdapter is not yet implemented")

    def translate_assignment(self, venue_event: dict) -> Optional[AssignmentEvent]:
        raise NotImplementedError("SchwabOptionsAdapter is not yet implemented")

    def translate_exercise(self, venue_event: dict) -> Optional[ExerciseEvent]:
        raise NotImplementedError("SchwabOptionsAdapter is not yet implemented")

    def translate_expiration(self, venue_event: dict) -> Optional[ExpirationEvent]:
        raise NotImplementedError("SchwabOptionsAdapter is not yet implemented")
