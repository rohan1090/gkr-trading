"""Session report model for options-capable trading sessions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from gkr_trading.core.operator_controls import OperatorAlert
from gkr_trading.core.reconciliation_model import OptionsReconciliationSnapshot


@dataclass
class OptionsSessionReport:
    """End-of-session report covering orders, lifecycle events, P&L, and risk."""
    session_id: str
    venue: str
    session_start_ns: int
    session_end_ns: int
    # Order summary
    single_leg_orders_submitted: int = 0
    single_leg_orders_filled: int = 0
    single_leg_orders_canceled: int = 0
    single_leg_orders_rejected: int = 0
    single_leg_orders_timed_out: int = 0
    spread_orders_submitted: int = 0
    spread_orders_filled: int = 0
    spread_orders_partially_filled: int = 0
    # Lifecycle events
    assignments_received: int = 0
    exercises_processed: int = 0
    expirations_processed: int = 0
    # P&L
    realized_pnl_cents: int = 0
    fees_paid_cents: int = 0
    premiums_paid_cents: int = 0
    premiums_received_cents: int = 0
    # Risk
    max_margin_requirement_cents: int = 0
    max_undefined_risk_positions: int = 0
    expiry_window_blocks: int = 0
    assignment_halts_triggered: int = 0
    # Audit
    reconciliation_snapshots: List[OptionsReconciliationSnapshot] = field(default_factory=list)
    alerts_raised: List[OperatorAlert] = field(default_factory=list)
    replay_validation_passed: bool = False
    shadow_orders_generated: int = 0
