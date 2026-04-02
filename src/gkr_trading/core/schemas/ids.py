from __future__ import annotations

import hashlib
import uuid
from decimal import Decimal
from typing import NewType

InstrumentId = NewType("InstrumentId", str)
SessionId = NewType("SessionId", str)
OrderId = NewType("OrderId", str)
IntentId = NewType("IntentId", str)
FillId = NewType("FillId", str)


def new_instrument_id() -> InstrumentId:
    return InstrumentId(str(uuid.uuid4()))


def new_session_id() -> SessionId:
    return SessionId(str(uuid.uuid4()))


def new_order_id() -> OrderId:
    return OrderId(str(uuid.uuid4()))


def new_intent_id() -> IntentId:
    return IntentId(str(uuid.uuid4()))


def fill_id_from_broker_execution(broker_execution_id: str) -> FillId:
    """Stable id for exchange/broker execution reports (e.g. Alpaca fill id)."""
    return FillId(f"exec:{broker_execution_id}")


def deterministic_fill_id_v1(
    order_id: str,
    fill_ts_utc: str,
    fill_qty: Decimal,
    fill_price: Decimal,
    fees: Decimal,
    *,
    salt: str = "",
) -> FillId:
    """Deterministic synthetic fill id for paper/backtest when no broker execution id exists."""
    raw = "|".join(
        (order_id, fill_ts_utc, str(fill_qty), str(fill_price), str(fees), salt)
    ).encode()
    return FillId("v1:" + hashlib.sha256(raw).hexdigest()[:40])


def resolve_fill_id(
    *,
    fill_id: FillId | None,
    broker_execution_id: str | None,
    order_id: OrderId,
    fill_ts_utc: str,
    fill_qty: Decimal,
    fill_price: Decimal,
    fees: Decimal,
    salt: str = "",
    synthetic_leg_key: str = "default",
) -> FillId:
    """
    Canonical fill identity policy:
    - If `broker_execution_id` is set, canonical id is always `exec:{broker_execution_id}`.
    - If `fill_id` is also set, it must match that canonical id (no silent override).
    - Otherwise (synthetic / paper / backtest): deterministic hash with non-empty
      `synthetic_leg_key` plus optional `salt` (dedupe_salt); callers must vary
      `synthetic_leg_key` across multiple fills for the same order to avoid collisions.
    """
    if broker_execution_id:
        expected = fill_id_from_broker_execution(broker_execution_id)
        if fill_id is not None and str(fill_id) != str(expected):
            raise ValueError(
                "fill_id must match exec:{broker_execution_id} when both are set; "
                f"got fill_id={fill_id!r} broker_execution_id={broker_execution_id!r}"
            )
        return expected
    if fill_id is not None:
        return fill_id
    if not synthetic_leg_key.strip():
        raise ValueError(
            "synthetic_leg_key must be non-empty for synthetic (non-broker) fill identity"
        )
    merged_salt = f"{salt}|leg:{synthetic_leg_key.strip()}"
    return deterministic_fill_id_v1(
        str(order_id), fill_ts_utc, fill_qty, fill_price, fees, salt=merged_salt
    )
