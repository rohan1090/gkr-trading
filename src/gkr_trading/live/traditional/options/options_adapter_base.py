"""OptionsCapableAdapterMixin — extends VenueAdapter for options.

Options capability extensions must not pollute the generic VenueAdapter interface.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.options_lifecycle import AssignmentEvent, ExerciseEvent, ExpirationEvent


class OptionsCapableAdapterMixin(ABC):
    """Mixin for venue adapters that support options trading.

    Keeps options-specific methods separate from the generic VenueAdapter.
    """

    @abstractmethod
    def get_options_positions(self) -> List[dict]:
        """Get all current options positions from venue."""
        ...

    @abstractmethod
    def get_pending_assignments(self) -> List[str]:
        """Get OCC symbols with pending assignment notifications."""
        ...

    @abstractmethod
    def get_expiring_today(self) -> List[str]:
        """Get OCC symbols expiring today."""
        ...

    @abstractmethod
    def translate_assignment(self, venue_event: dict) -> Optional[AssignmentEvent]:
        """Translate venue-specific assignment notification to canonical event."""
        ...

    @abstractmethod
    def translate_exercise(self, venue_event: dict) -> Optional[ExerciseEvent]:
        """Translate venue-specific exercise notification to canonical event."""
        ...

    @abstractmethod
    def translate_expiration(self, venue_event: dict) -> Optional[ExpirationEvent]:
        """Translate venue-specific expiration notification to canonical event."""
        ...
