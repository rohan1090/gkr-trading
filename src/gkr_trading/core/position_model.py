"""Position model — equity and options position records.

Key design decision: Options positions use long_contracts / short_contracts
(both >= 0), NOT a signed net quantity. Long and short carry fundamentally
different risk profiles and cannot be netted without losing information.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Tuple

from gkr_trading.core.instruments import OptionsRef


@dataclass(frozen=True)
class EquityPositionRecord:
    """Equity position. signed_qty positive = long, negative = short."""
    ticker: str
    venue: str
    signed_qty: int
    cost_basis_cents: int       # total cost, not per-share
    realized_pnl_cents: int
    status: Literal["open", "closed"]

    def __post_init__(self) -> None:
        if self.status == "open" and self.signed_qty == 0:
            raise ValueError("open position must have non-zero quantity")


@dataclass(frozen=True)
class OptionsContractRecord:
    """Single options contract position.

    long_contracts and short_contracts are both >= 0.
    A position cannot be simultaneously long and short the same contract.
    """
    instrument_ref: OptionsRef
    venue: str
    long_contracts: int
    short_contracts: int
    long_premium_paid_cents: int
    short_premium_received_cents: int
    realized_pnl_cents: int
    status: Literal["open", "closed", "expired", "assigned", "exercised"]
    has_undefined_risk: bool     # True for naked short calls

    def __post_init__(self) -> None:
        if self.long_contracts < 0:
            raise ValueError("long_contracts must be >= 0")
        if self.short_contracts < 0:
            raise ValueError("short_contracts must be >= 0")
        if self.long_contracts > 0 and self.short_contracts > 0:
            raise ValueError(
                "simultaneous long and short the same contract — reconciliation error"
            )

    @property
    def net_contracts(self) -> int:
        """Signed net position (positive = long, negative = short)."""
        return self.long_contracts - self.short_contracts

    @property
    def is_flat(self) -> bool:
        return self.long_contracts == 0 and self.short_contracts == 0


@dataclass(frozen=True)
class SpreadPositionRecord:
    """Phase 6+. Grouped exposure record for a multi-leg spread."""
    spread_id: str
    underlying: str
    legs: Tuple[OptionsContractRecord, ...]
    max_loss_cents: int
    max_gain_cents: int
    spread_type: str  # "vertical", "calendar", "iron_condor", etc.
    integrity_status: Literal["intact", "partial_fill", "one_leg_closed", "broken"]
    created_at_ns: int


@dataclass
class AccountSnapshot:
    """Point-in-time snapshot of full account state."""
    snapshot_id: str
    session_id: str
    timestamp_ns: int
    venue: str
    equity_positions: Tuple[EquityPositionRecord, ...]
    options_positions: Tuple[OptionsContractRecord, ...]
    spread_positions: Tuple[SpreadPositionRecord, ...]
    cash_balance_cents: int
    options_buying_power_cents: int
    margin_requirement_cents: int
    unsettled_pnl_cents: int
