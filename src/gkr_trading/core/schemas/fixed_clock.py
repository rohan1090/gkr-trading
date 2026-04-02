from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class FixedClock:
    """Deterministic clock for backtests."""

    current: datetime

    def utc_now(self) -> datetime:
        return self.current

    def set(self, t: datetime) -> None:
        self.current = t
