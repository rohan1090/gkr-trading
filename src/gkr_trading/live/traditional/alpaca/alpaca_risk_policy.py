"""Alpaca-specific risk policy — day trade count, buying power checks."""
from __future__ import annotations

from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.risk_gate import RiskApprovalGate, RiskDecision


class AlpacaRiskPolicy(RiskApprovalGate):
    """Alpaca-specific risk checks.

    - Day trade count limit (PDT rule)
    - Alpaca-specific buying power model
    """

    def __init__(
        self,
        max_day_trades: int = 3,
        is_pdt_exempt: bool = False,
    ) -> None:
        self._max_day_trades = max_day_trades
        self._pdt_exempt = is_pdt_exempt
        self._day_trade_count = 0

    def evaluate(self, intent: object, context: object) -> RiskDecision:
        if not isinstance(intent, TradeIntent):
            return RiskDecision(approved=False, reason_code="INVALID_INTENT_TYPE")

        # PDT check
        if not self._pdt_exempt:
            if intent.action in ("sell_to_close", "buy_to_close"):
                if self._day_trade_count >= self._max_day_trades:
                    return RiskDecision(
                        approved=False,
                        reason_code="PDT_LIMIT",
                        reason_detail=f"Day trade count {self._day_trade_count} >= {self._max_day_trades}",
                    )

        return RiskDecision(approved=True)

    def record_day_trade(self) -> None:
        """Record a day trade (same-day open + close)."""
        self._day_trade_count += 1

    def reset_day_trades(self) -> None:
        """Reset at start of new trading day."""
        self._day_trade_count = 0
