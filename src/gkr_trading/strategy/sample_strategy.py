from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import new_intent_id
from gkr_trading.data.market_store.repository import BarRow


class SampleBarCrossStrategy:
    """Emit BUY on short-term bounce (down then up); SELL on rally then down — two-bar pattern."""

    name = "sample_bar_cross"

    def __init__(self, trade_qty: Decimal = Decimal("10")) -> None:
        self._qty = trade_qty

    def on_bar(self, bar: BarRow, history: tuple[BarRow, ...]) -> TradeIntent | None:
        if len(history) < 2:
            return None
        p2, p1 = history[-2], history[-1]
        if p1.close < p2.close and bar.close > p1.close:
            return TradeIntent(
                intent_id=new_intent_id(),
                instrument_id=bar.instrument_id,
                side=OrderSide.BUY,
                quantity=self._qty,
                order_type=OrderType.MARKET,
                strategy_name=self.name,
            )
        if p1.close > p2.close and bar.close < p1.close:
            return TradeIntent(
                intent_id=new_intent_id(),
                instrument_id=bar.instrument_id,
                side=OrderSide.SELL,
                quantity=self._qty,
                order_type=OrderType.MARKET,
                strategy_name=self.name,
            )
        return None
