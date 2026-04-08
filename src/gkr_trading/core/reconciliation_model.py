"""Reconciliation model — breaks and snapshots.

ReconciliationBreak includes severity to distinguish warnings from
blocking breaks that halt order submission.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

from gkr_trading.core.position_model import EquityPositionRecord, OptionsContractRecord


@dataclass(frozen=True)
class ReconciliationBreak:
    """A single discrepancy between local and venue state."""
    field: str
    local_value: str
    venue_value: str
    break_type: Literal[
        "position", "cash", "options_bp", "margin", "open_orders",
        "assignment_pending", "expiration_pending", "spread_integrity"
    ]
    severity: Literal["warning", "blocking"]


@dataclass
class OptionsReconciliationSnapshot:
    """Full reconciliation snapshot for options-capable accounts."""
    snapshot_id: str
    session_id: str
    timestamp_ns: int
    trigger: Literal["startup", "shutdown", "on_demand", "post_reconnect", "post_expiry"]
    local_equity_positions: Tuple[EquityPositionRecord, ...]
    venue_equity_positions: Tuple[EquityPositionRecord, ...]
    local_options_positions: Tuple[OptionsContractRecord, ...]
    venue_options_positions: Tuple[OptionsContractRecord, ...]
    pending_assignments: Tuple[str, ...]
    pending_expirations: Tuple[str, ...]
    local_cash_cents: int
    venue_cash_cents: int
    local_options_buying_power_cents: int
    venue_options_buying_power_cents: int
    local_margin_requirement_cents: int
    venue_margin_requirement_cents: int
    breaks: Tuple[ReconciliationBreak, ...]
    status: Literal["clean", "break_detected", "acknowledged"]

    def has_blocking_breaks(self) -> bool:
        return any(b.severity == "blocking" for b in self.breaks)
