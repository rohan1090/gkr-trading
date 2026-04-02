from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.builders import fill_received, order_acknowledged
from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId


def simulate_immediate_fill(
    *,
    order_id: OrderId,
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: Decimal,
    order_type: OrderType,
    limit_price: Decimal | None,
    fill_price: Decimal,
    bar_ts_utc: str,
    occurred_at_utc: str,
    fees: Decimal = Decimal("0"),
) -> list[CanonicalEvent]:
    """Backtest-only: instant full fill at `fill_price` (e.g. bar close)."""
    ack = order_acknowledged(order_id, occurred_at_utc, broker_order_id=f"SIM-{order_id}")
    fill = fill_received(
        order_id,
        instrument_id,
        side,
        quantity,
        fill_price,
        bar_ts_utc,
        occurred_at_utc,
        fees=fees,
        synthetic_leg_key="bt_immediate",
    )
    return [ack, fill]
