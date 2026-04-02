"""JSON-safe serialization for SubmitRequest (Alpaca broker state persistence)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId
from gkr_trading.live.broker_adapter import SubmitRequest


def submit_request_to_jsonable(req: SubmitRequest) -> dict[str, Any]:
    return {
        "order_id": str(req.order_id),
        "instrument_id": str(req.instrument_id),
        "side": req.side.value,
        "quantity": str(req.quantity),
        "order_type": req.order_type.value,
        "limit_price": str(req.limit_price) if req.limit_price is not None else None,
        "executable_broker_symbol": req.executable_broker_symbol,
        "context_ts_utc": req.context_ts_utc,
    }


def submit_request_from_jsonable(d: dict[str, Any]) -> SubmitRequest:
    lp = d.get("limit_price")
    return SubmitRequest(
        order_id=OrderId(str(d["order_id"])),
        instrument_id=InstrumentId(str(d["instrument_id"])),
        side=OrderSide(str(d["side"])),
        quantity=Decimal(str(d["quantity"])),
        order_type=OrderType(str(d["order_type"])),
        limit_price=Decimal(str(lp)) if lp is not None else None,
        executable_broker_symbol=(
            str(d["executable_broker_symbol"]) if d.get("executable_broker_symbol") else None
        ),
        context_ts_utc=str(d["context_ts_utc"]) if d.get("context_ts_utc") else None,
    )
