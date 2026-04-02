from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PortfolioAnomaly:
    """Recorded when an event sequence is permissively applied but violates invariants."""

    code: str
    message: str
    event_type: str | None = None
    event_index: int | None = None
