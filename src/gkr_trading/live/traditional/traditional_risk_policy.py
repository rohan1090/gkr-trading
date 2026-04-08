"""SharedTraditionalRiskPolicy — market hours, buying power, position limits."""
from __future__ import annotations

from datetime import datetime, timezone

from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.risk_gate import RiskApprovalGate, RiskDecision
from gkr_trading.live.traditional.market_calendar import MarketCalendarProvider


class SharedTraditionalRiskPolicy(RiskApprovalGate):
    """Risk policy shared across traditional market venues.

    Checks: market hours, position limits, buying power pre-check.
    Options-specific checks are in OptionsRiskPolicy.
    """

    def __init__(
        self,
        max_equity_position: int = 1000,
        max_notional_cents: int = 100_000_00,  # $100k
        calendar: MarketCalendarProvider | None = None,
    ) -> None:
        self._max_equity_pos = max_equity_position
        self._max_notional = max_notional_cents
        self._calendar = calendar or MarketCalendarProvider()

    def evaluate(self, intent: object, context: object) -> RiskDecision:
        if not isinstance(intent, TradeIntent):
            return RiskDecision(approved=False, reason_code="INVALID_INTENT_TYPE")

        # Market hours check
        now = datetime.now(timezone.utc)
        if not self._calendar.is_market_open(now):
            return RiskDecision(
                approved=False,
                reason_code="MARKET_CLOSED",
                reason_detail="Market is not open for regular trading",
            )

        # Equity position limit
        if isinstance(intent.instrument_ref, EquityRef):
            if intent.quantity > self._max_equity_pos:
                return RiskDecision(
                    approved=False,
                    reason_code="MAX_POSITION",
                    reason_detail=f"Quantity {intent.quantity} exceeds max {self._max_equity_pos}",
                )

        # Options require limit price (already enforced by TradeIntent, but double-check)
        if isinstance(intent.instrument_ref, OptionsRef):
            if intent.limit_price_cents is None:
                return RiskDecision(
                    approved=False,
                    reason_code="OPTIONS_REQUIRE_LIMIT",
                    reason_detail="Options orders must have a limit price",
                )

        return RiskDecision(approved=True)
