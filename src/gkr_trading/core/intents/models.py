from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, IntentId


class TradeIntent(BaseModel):
    model_config = {"frozen": True}

    intent_id: IntentId
    instrument_id: InstrumentId
    side: OrderSide
    quantity: Decimal = Field(gt=0)
    order_type: OrderType = OrderType.MARKET
    limit_price: Decimal | None = None
    strategy_name: str = "sample"
    client_tag: str | None = None
