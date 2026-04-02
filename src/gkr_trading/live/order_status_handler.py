from __future__ import annotations

from gkr_trading.core.events.builders import order_acknowledged
from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.schemas.ids import OrderId


def ack_for_submit(broker_order_id: str, order_id: OrderId, occurred_at_utc: str) -> CanonicalEvent:
    return order_acknowledged(order_id, occurred_at_utc, broker_order_id=broker_order_id)
