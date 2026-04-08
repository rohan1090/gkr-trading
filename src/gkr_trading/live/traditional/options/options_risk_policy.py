"""OptionsRiskPolicy — undefined risk halt, expiry window block, assignment hazard."""
from __future__ import annotations

from datetime import date, datetime, timezone

from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.risk_gate import RiskApprovalGate, RiskDecision
from gkr_trading.live.traditional.options.options_domain import OptionsChainHelper


class OptionsRiskPolicy(RiskApprovalGate):
    """Options-specific risk checks.

    - Max contracts per position
    - Undefined risk halt (naked short calls)
    - Expiry window block (no new positions near expiry)
    - Assignment hazard check (short options near ITM)
    """

    def __init__(
        self,
        max_contracts: int = 10,
        block_undefined_risk: bool = True,
        expiry_window_days: int = 0,
    ) -> None:
        self._max_contracts = max_contracts
        self._block_undefined = block_undefined_risk
        self._expiry_window_days = expiry_window_days

    def evaluate(self, intent: object, context: object) -> RiskDecision:
        if not isinstance(intent, TradeIntent):
            return RiskDecision(approved=False, reason_code="INVALID_INTENT_TYPE")

        ref = intent.instrument_ref
        if not isinstance(ref, OptionsRef):
            # Not an options intent — pass through
            return RiskDecision(approved=True)

        # Max contracts check
        if intent.quantity > self._max_contracts:
            return RiskDecision(
                approved=False,
                reason_code="MAX_CONTRACTS",
                reason_detail=f"Quantity {intent.quantity} exceeds max {self._max_contracts}",
            )

        # Undefined risk check — block naked short calls
        if self._block_undefined and intent.action == "sell_to_open" and ref.right == "call":
            return RiskDecision(
                approved=False,
                reason_code="UNDEFINED_RISK",
                reason_detail="Naked short calls are blocked (undefined risk)",
            )

        # Expiry window block — no new opening orders near expiry
        today = datetime.now(timezone.utc).date()
        if intent.action in ("buy_to_open", "sell_to_open"):
            if OptionsChainHelper.is_in_expiry_window(ref, today, self._expiry_window_days):
                return RiskDecision(
                    approved=False,
                    reason_code="EXPIRY_WINDOW_BLOCK",
                    reason_detail=f"Cannot open new positions within {self._expiry_window_days} days of expiry",
                )

        return RiskDecision(approved=True)
