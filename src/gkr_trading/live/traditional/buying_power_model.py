"""BuyingPowerModel ABC — cash + options margin."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class BuyingPowerSnapshot:
    """Point-in-time buying power state."""
    cash_cents: int
    equity_buying_power_cents: int
    options_buying_power_cents: int
    margin_used_cents: int
    margin_available_cents: int


class BuyingPowerModel(ABC):
    """Abstract buying power model. Implementations are venue-specific."""

    @abstractmethod
    def compute(self, account_data: dict) -> BuyingPowerSnapshot:
        """Compute buying power from venue account data."""
        ...

    @abstractmethod
    def can_afford_equity(self, cost_cents: int, snapshot: BuyingPowerSnapshot) -> bool:
        """Check if equity purchase is affordable."""
        ...

    @abstractmethod
    def can_afford_options(self, premium_cents: int, snapshot: BuyingPowerSnapshot) -> bool:
        """Check if options purchase is affordable."""
        ...
