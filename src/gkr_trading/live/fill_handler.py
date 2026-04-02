from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.builders import fill_received
from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.schemas.enums import OrderSide
from gkr_trading.core.schemas.ids import InstrumentId, OrderId


def synthetic_fill(
    order_id: OrderId,
    instrument_id: InstrumentId,
    side: OrderSide,
    qty: Decimal,
    price: Decimal,
    ts_utc: str,
    occurred_at_utc: str,
) -> CanonicalEvent:
    return fill_received(
        order_id,
        instrument_id,
        side,
        qty,
        price,
        ts_utc,
        occurred_at_utc,
        synthetic_leg_key="paper",
    )
