from __future__ import annotations

from datetime import datetime
from typing import Protocol


class Clock(Protocol):
    def utc_now(self) -> datetime: ...
