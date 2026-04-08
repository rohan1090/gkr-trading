"""Options-capable TradeIntent and SpreadIntent models.

New module — does NOT replace core/intents/models.py which remains for
backward compatibility with existing equity-only code paths.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Tuple

from gkr_trading.core.instruments import InstrumentRef, OptionsRef


# Valid actions for options-aware trading
Action = Literal["buy_to_open", "sell_to_open", "buy_to_close", "sell_to_close"]
VenueClass = Literal["traditional", "prediction"]
TimeInForce = Literal["day", "gtc", "ioc", "fok"]


@dataclass(frozen=True)
class TradeIntent:
    """Venue-agnostic, strategy-emitted intent object.

    Strategy must not include any venue, broker, or OCC-specific fields.
    """
    intent_id: str
    strategy_id: str
    session_id: str
    venue_class: VenueClass
    instrument_ref: InstrumentRef
    action: Action
    quantity: int
    limit_price_cents: Optional[int]
    time_in_force: TimeInForce
    created_at_ns: int

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.limit_price_cents is not None and self.limit_price_cents <= 0:
            raise ValueError("limit_price_cents must be positive when set")
        # Options REQUIRE limit orders — market orders on options are forbidden
        if isinstance(self.instrument_ref, OptionsRef):
            if self.limit_price_cents is None:
                raise ValueError(
                    "options require a limit price — market orders on options are forbidden"
                )


@dataclass(frozen=True)
class SpreadLeg:
    """Single leg of a multi-leg spread."""
    instrument_ref: OptionsRef
    action: Action
    ratio_quantity: int  # typically 1:1; ratio spreads use >1

    def __post_init__(self) -> None:
        if self.ratio_quantity <= 0:
            raise ValueError("ratio_quantity must be positive")


@dataclass(frozen=True)
class SpreadIntent:
    """Phase 6+ only. Not used until SpreadIntegrityChecker is complete.

    Multi-leg spreads are NOT a list of TradeIntent objects.
    SpreadIntent is an irreducible unit — rejected or accepted as a whole.
    """
    intent_id: str
    strategy_id: str
    session_id: str
    venue_class: Literal["traditional"]
    legs: Tuple[SpreadLeg, ...]
    net_limit_price_cents: int  # net debit (positive) or net credit (negative)
    time_in_force: Literal["day"]  # multi-leg: day only
    created_at_ns: int

    def __post_init__(self) -> None:
        if len(self.legs) < 2:
            raise ValueError("SpreadIntent requires at least 2 legs")
        if not all(isinstance(leg.instrument_ref, OptionsRef) for leg in self.legs):
            raise ValueError("all SpreadIntent legs must be options")
        underlyings = {leg.instrument_ref.underlying for leg in self.legs}
        if len(underlyings) != 1:
            raise ValueError("all legs of a spread must share the same underlying")
