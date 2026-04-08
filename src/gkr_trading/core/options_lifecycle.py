"""Options lifecycle events — Assignment, Exercise, Expiration.

These are NOT fills. They are distinct domain events with different
semantics, accounting treatment, and reconciliation behavior.

- Assignment: external event from OCC, creates equity position delta
- Exercise: initiated by this account or auto-exercise, creates equity delta
- Expiration: option expires worthless, no cash flow, position removed
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from gkr_trading.core.instruments import OptionsRef


@dataclass(frozen=True)
class AssignmentEvent:
    """NOT a fill. Assignment is an external venue event delivered asynchronously.

    For short calls: account must deliver shares (or cover).
    For short puts: account must purchase shares at strike.
    Assignment creates a NEW equity position or modifies an existing one.
    The options position is simultaneously closed.
    """
    event_id: str
    session_id: str
    seq_no: int
    instrument_ref: OptionsRef
    venue: str
    contracts_assigned: int
    strike_cents: int
    right: Literal["call", "put"]
    resulting_equity_delta: int   # +contracts*100 (put assigned) or -contracts*100 (call assigned)
    equity_underlying: str
    assignment_price_cents: int   # strike price
    effective_date: str           # YYYY-MM-DD, settlement T+1
    source: Literal["auto", "manual"]  # "auto" (OCC) | "manual" (holder exercised)
    timestamp_ns: int
    requires_operator_review: bool  # True for naked short assignments

    def __post_init__(self) -> None:
        if self.contracts_assigned <= 0:
            raise ValueError("contracts_assigned must be positive")
        if self.strike_cents <= 0:
            raise ValueError("strike_cents must be positive")


@dataclass(frozen=True)
class ExerciseEvent:
    """Long position exercise. Initiated by this account.

    For long calls: account receives shares at strike.
    For long puts: account delivers shares at strike.
    NOT a fill. NOT the same as closing the option via a sell order.
    """
    event_id: str
    session_id: str
    seq_no: int
    instrument_ref: OptionsRef
    venue: str
    contracts_exercised: int
    strike_cents: int
    right: Literal["call", "put"]
    resulting_equity_delta: int   # +contracts*100 (call) or -contracts*100 (put)
    equity_underlying: str
    effective_date: str
    initiated_by: Literal["system", "operator"]  # "system" (auto) | "operator" (manual)
    timestamp_ns: int

    def __post_init__(self) -> None:
        if self.contracts_exercised <= 0:
            raise ValueError("contracts_exercised must be positive")


@dataclass(frozen=True)
class ExpirationEvent:
    """Option expired worthless. OTM at expiration, no exercise.

    NOT a fill. Premium paid is a sunk cost — no new cash flow.
    The options position is removed. No equity position change.
    Must be recorded durably so reconciliation knows the position was
    intentionally removed by expiration, not by a missing fill event.
    """
    event_id: str
    session_id: str
    seq_no: int
    instrument_ref: OptionsRef
    venue: str
    contracts_expired: int
    moneyness_at_expiry: Literal["otm", "atm"]
    premium_paid_cents: int       # cost basis of expired contracts (long only)
    premium_received_cents: int   # premium received (short only, now kept)
    expired_at_ns: int
    expiry_type: Literal["standard_monthly", "weekly", "quarterly", "other"]

    def __post_init__(self) -> None:
        if self.contracts_expired <= 0:
            raise ValueError("contracts_expired must be positive")
