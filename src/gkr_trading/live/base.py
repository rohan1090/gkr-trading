"""VenueAdapter ABC — abstract interface for venue integration.

This is the new blueprint interface. The existing broker_adapter.py
is preserved for backward compatibility with the equity-only path.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from gkr_trading.core.instruments import InstrumentRef
from gkr_trading.core.order_model import OrderStatus


@dataclass(frozen=True)
class SubmissionRequest:
    """Venue-agnostic submission request."""
    client_order_id: str
    instrument_ref: InstrumentRef
    action: str
    quantity: int
    limit_price_cents: Optional[int]
    time_in_force: str
    venue: str


@dataclass(frozen=True)
class SubmissionResponse:
    """Result of a venue submission attempt."""
    client_order_id: str
    venue_order_id: Optional[str]
    success: bool
    rejected: bool = False
    reject_reason: Optional[str] = None
    timeout: bool = False
    raw_response: Optional[str] = None  # JSON string for raw_response_archive


@dataclass(frozen=True)
class VenuePosition:
    """Position as reported by venue."""
    instrument_key: str  # canonical_key from InstrumentRef
    quantity: int
    avg_entry_price_cents: Optional[int] = None
    market_value_cents: Optional[int] = None


@dataclass(frozen=True)
class VenueAccountInfo:
    """Account info as reported by venue."""
    cash_cents: int
    buying_power_cents: int
    options_buying_power_cents: int = 0
    margin_requirement_cents: int = 0


class VenueAdapter(ABC):
    """Abstract venue adapter interface.

    Implementations in live/traditional/alpaca/, live/traditional/schwab/, etc.
    """

    @abstractmethod
    def submit_order(self, request: SubmissionRequest) -> SubmissionResponse:
        """Submit an order to the venue. Returns synchronous response."""
        ...

    @abstractmethod
    def cancel_order(self, client_order_id: str) -> bool:
        """Cancel an order. Returns True if cancel acknowledged."""
        ...

    @abstractmethod
    def get_order_status(self, client_order_id: str) -> Optional[OrderStatus]:
        """Query current status of an order at venue."""
        ...

    @abstractmethod
    def get_positions(self) -> List[VenuePosition]:
        """Get all current positions from venue."""
        ...

    @abstractmethod
    def get_account(self) -> VenueAccountInfo:
        """Get account info from venue."""
        ...

    @property
    @abstractmethod
    def venue_name(self) -> str:
        """Unique identifier for this venue."""
        ...
