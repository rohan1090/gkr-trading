"""Canonical fill model.

FillEvent applies to equities and single-leg options fills only.
SpreadFillEvent/SpreadLegFillEvent are Phase 6+ stubs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from gkr_trading.core.instruments import InstrumentRef, OptionsRef


@dataclass(frozen=True)
class FillEvent:
    """Canonical fill. Equities and single-leg options only."""
    event_id: str
    session_id: str
    seq_no: int
    client_order_id: str
    venue_fill_id: str
    instrument_ref: InstrumentRef
    venue: str
    action: str
    quantity: int
    price_cents: int
    fee_cents: int
    is_taker: bool
    timestamp_ns: int

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("fill quantity must be positive")
        if self.price_cents < 0:
            raise ValueError("fill price_cents must be non-negative")


@dataclass(frozen=True)
class SpreadLegFillEvent:
    """Phase 6+. Emitted for each leg of a multi-leg fill.

    NOT a FillEvent subclass — spread leg fills have different accounting semantics.
    """
    event_id: str
    session_id: str
    seq_no: int
    spread_fill_id: str
    leg_client_order_id: str
    venue_fill_id: str
    instrument_ref: OptionsRef
    action: str
    quantity: int
    price_cents: int
    fee_cents: int
    timestamp_ns: int


@dataclass(frozen=True)
class SpreadFillEvent:
    """Phase 6+. Parent fill for a multi-leg spread order."""
    event_id: str
    session_id: str
    seq_no: int
    spread_order_id: str
    net_price_cents: int
    quantity: int
    timestamp_ns: int
    leg_fills: Tuple[SpreadLegFillEvent, ...]
