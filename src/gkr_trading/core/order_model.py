"""Order model — VenueOrder, OrderStatus, SpreadOrder.

OrderStatus includes UNKNOWN for crash recovery.
VenueOrder tracks the full lifecycle of a submitted order.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from gkr_trading.core.instruments import InstrumentRef


class OrderStatus(Enum):
    PENDING_LOCAL = "pending_local"       # written to EventStore, not yet sent
    SUBMITTED = "submitted"               # API call made, awaiting venue ack
    PENDING_NEW = "pending_new"           # venue received, not yet active
    RESTING = "resting"                   # resting on book
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"                   # for options: also covers expiration at market close
    UNKNOWN = "unknown"                   # timeout/crash recovery state

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATUSES


_TERMINAL_STATUSES = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
})

# Legal status transitions
VALID_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING_LOCAL: frozenset({OrderStatus.SUBMITTED, OrderStatus.UNKNOWN, OrderStatus.REJECTED}),
    OrderStatus.SUBMITTED: frozenset({OrderStatus.PENDING_NEW, OrderStatus.RESTING, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.UNKNOWN}),
    OrderStatus.PENDING_NEW: frozenset({OrderStatus.RESTING, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.UNKNOWN}),
    OrderStatus.RESTING: frozenset({OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.UNKNOWN}),
    OrderStatus.PARTIALLY_FILLED: frozenset({OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.UNKNOWN}),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
    OrderStatus.UNKNOWN: frozenset({OrderStatus.SUBMITTED, OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED}),
}


def validate_transition(current: OrderStatus, target: OrderStatus) -> bool:
    """Check if a status transition is legal."""
    return target in VALID_TRANSITIONS.get(current, frozenset())


@dataclass(frozen=True)
class VenueOrder:
    """Canonical order record tracking full lifecycle."""
    client_order_id: str
    venue_order_id: Optional[str]
    intent_id: str
    session_id: str
    instrument_ref: InstrumentRef
    venue: str
    action: str
    status: OrderStatus
    filled_qty: int
    remaining_qty: int
    avg_fill_price_cents: Optional[int]
    created_at_ns: int
    updated_at_ns: int
    # Options-specific (None for equities)
    position_intent: Optional[str] = None
    # Spread-specific (None for single-leg)
    spread_order_id: Optional[str] = None


@dataclass(frozen=True)
class SpreadOrder:
    """Phase 6+. Parent record linking all spread legs."""
    spread_order_id: str
    intent_id: str
    session_id: str
    venue: str
    legs: Tuple[VenueOrder, ...]
    net_fill_price_cents: Optional[int]
    status: OrderStatus
    created_at_ns: int
    updated_at_ns: int
