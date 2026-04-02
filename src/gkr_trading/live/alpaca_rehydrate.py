"""Rebuild Alpaca adapter tracking from canonical session events (restart / no JSON blob)."""

from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events import CanonicalEvent
from gkr_trading.core.events.types import EventType
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.ids import OrderId
from gkr_trading.live.broker_adapter import SubmitRequest


def rehydrate_tracked_orders_from_events(
    events: list[CanonicalEvent],
    starting_cash: Decimal,
) -> tuple[dict[str, SubmitRequest], dict[str, str], list[str]]:
    """
    Open portfolio orders + ORDER_ACK broker ids -> adapter tracking maps.

    Returns (client_order_id -> SubmitRequest with remaining qty, client -> alpaca_order_id,
    anomaly messages). Does not mutate portfolio; read-only fold for open order detection.
    """
    rr = replay_portfolio_state(events, starting_cash)
    ack: dict[str, str] = {}
    for e in events:
        if e.event_type != EventType.ORDER_ACKNOWLEDGED:
            continue
        p = e.payload
        oid = str(p.order_id)
        if p.broker_order_id:
            ack[oid] = str(p.broker_order_id)

    anomalies: list[str] = []
    by_client: dict[str, SubmitRequest] = {}
    alpaca_map: dict[str, str] = {}
    for _key, oo in rr.state.open_orders.items():
        oid_str = str(oo.order_id)
        bid = ack.get(oid_str)
        if not bid:
            anomalies.append(f"REHYDRATE_OPEN_ORDER_MISSING_BROKER_ACK: order_id={oid_str}")
            continue
        by_client[oid_str] = SubmitRequest(
            order_id=OrderId(oid_str),
            instrument_id=oo.instrument_id,
            side=oo.side,
            quantity=oo.remaining_qty,
            order_type=oo.order_type,
            limit_price=oo.limit_price,
            executable_broker_symbol=None,
            context_ts_utc=None,
        )
        alpaca_map[oid_str] = bid
    return by_client, alpaca_map, anomalies
