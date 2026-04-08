"""FillTranslator ABC — translate venue-specific fills to canonical FillEvent."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from gkr_trading.core.fills import FillEvent


class FillTranslator(ABC):
    """Translate venue-specific fill payloads into canonical FillEvent.

    Implementations in live/traditional/alpaca/, etc.
    """

    @abstractmethod
    def translate_fill(self, venue_payload: dict) -> Optional[FillEvent]:
        """Translate a venue fill payload into a canonical FillEvent.

        Returns None if the payload is not a fill (e.g., assignment, exercise).
        """
        ...
