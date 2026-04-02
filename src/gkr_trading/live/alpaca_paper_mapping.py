"""
Pure Alpaca JSON -> normalized broker facts (unit-tested, no I/O).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from gkr_trading.core.schemas.enums import OrderSide
from gkr_trading.core.schemas.ids import InstrumentId, OrderId
from gkr_trading.live.broker_adapter import (
    BrokerFillFact,
    BrokerOrderCancelledFact,
    BrokerOrderRejectedFact,
    SubmitRequest,
)


class AlpacaMalformedPayloadError(ValueError):
    """Alpaca payload missing fields required for normalized facts."""


def _to_utc_z(ts: str | None, *, fallback: str) -> str:
    from datetime import UTC, datetime

    if not ts or not str(ts).strip():
        return fallback
    s = str(ts).strip().replace("Z", "+00:00")
    d = datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    d = d.astimezone(UTC)
    return d.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def _dec(x: Any, *, field: str) -> Decimal:
    if x is None:
        raise AlpacaMalformedPayloadError(f"missing numeric field {field}")
    return Decimal(str(x))


def fill_activity_to_broker_fill_fact(
    activity: dict[str, Any],
    *,
    submit: SubmitRequest,
    fallback_ts: str,
) -> BrokerFillFact:
    """Map Alpaca account activity (type FILL) to BrokerFillFact."""
    aid = activity.get("id")
    if not aid:
        raise AlpacaMalformedPayloadError("FILL activity missing id")
    oid = activity.get("order_id")
    if not oid:
        raise AlpacaMalformedPayloadError("FILL activity missing order_id")
    qty = _dec(activity.get("qty"), field="qty")
    price = _dec(activity.get("price") or activity.get("fill_price"), field="price")
    txn = _to_utc_z(activity.get("transaction_time"), fallback=fallback_ts)
    side_raw = (activity.get("side") or "").lower()
    if side_raw not in ("buy", "sell"):
        side_raw = submit.side.value
    side = OrderSide.BUY if side_raw == "buy" else OrderSide.SELL
    return BrokerFillFact(
        client_order_id=submit.order_id,
        instrument_id=submit.instrument_id,
        side=side,
        quantity=qty,
        price=price,
        fees=Decimal("0"),
        fill_ts_utc=txn,
        occurred_at_utc=txn,
        broker_execution_id=str(aid),
    )


def order_to_lifecycle_facts(
    order: dict[str, Any],
    *,
    submit: SubmitRequest,
    fallback_ts: str,
) -> list[BrokerOrderRejectedFact | BrokerOrderCancelledFact]:
    """Emit cancel/reject facts for terminal Alpaca order statuses (at most one per kind)."""
    st = (order.get("status") or "").lower()
    ts = _to_utc_z(
        order.get("updated_at") or order.get("submitted_at"),
        fallback=fallback_ts,
    )
    oid = OrderId(str(submit.order_id))
    if st == "rejected":
        msg = order.get("reject_reason") or order.get("status_description") or "rejected"
        return [
            BrokerOrderRejectedFact(
                client_order_id=oid,
                reason_code="ALPACA_REJECTED",
                occurred_at_utc=ts,
                reason_detail=str(msg) if msg else None,
            )
        ]
    if st in ("canceled", "cancelled", "expired", "done_for_day"):
        return [
            BrokerOrderCancelledFact(
                client_order_id=oid,
                occurred_at_utc=ts,
                reason_code=st.upper(),
                cancelled_qty=None,
            )
        ]
    return []
