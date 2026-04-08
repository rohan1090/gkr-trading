"""Venue capability registry — validates intent can be expressed at target venue."""
from __future__ import annotations

from enum import Enum
from typing import Dict, FrozenSet


class VenueCapability(Enum):
    EQUITY_TRADING = "equity_trading"
    OPTIONS_SINGLE_LEG = "options_single_leg"
    OPTIONS_SPREADS = "options_spreads"
    MARKET_ORDERS = "market_orders"
    LIMIT_ORDERS = "limit_orders"
    GTC_ORDERS = "gtc_orders"
    PAPER_TRADING = "paper_trading"
    LIVE_TRADING = "live_trading"
    WEBSOCKET_STREAMING = "websocket_streaming"
    REST_POLLING = "rest_polling"


class CapabilityRegistry:
    """Registry of venue capabilities for intent validation."""

    def __init__(self) -> None:
        self._venues: Dict[str, FrozenSet[VenueCapability]] = {}

    def register(self, venue: str, capabilities: FrozenSet[VenueCapability]) -> None:
        self._venues[venue] = capabilities

    def has_capability(self, venue: str, capability: VenueCapability) -> bool:
        caps = self._venues.get(venue)
        if caps is None:
            return False
        return capability in caps

    def get_capabilities(self, venue: str) -> FrozenSet[VenueCapability]:
        return self._venues.get(venue, frozenset())

    def validate_intent_feasible(self, venue: str, requires: FrozenSet[VenueCapability]) -> list[str]:
        """Returns list of missing capabilities, empty if all present."""
        caps = self._venues.get(venue, frozenset())
        missing = requires - caps
        return [c.value for c in missing]
