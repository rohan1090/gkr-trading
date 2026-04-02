"""Explicit event sequences for adversarial tests."""

from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.builders import order_submitted, session_started, session_stopped
from gkr_trading.core.events.builders import fill_received as fill_ev
from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId, SessionId

IID = InstrumentId("00000000-0000-4000-8000-00000000a001")


def base_submit_fill(
    oid: OrderId,
    *,
    qty: Decimal = Decimal("10"),
    price: Decimal = Decimal("100"),
    ts: str = "2024-01-01T12:00:00Z",
) -> list[CanonicalEvent]:
    return [
        order_submitted(oid, IID, OrderSide.BUY, qty, OrderType.MARKET, None, ts),
        fill_ev(oid, IID, OrderSide.BUY, qty, price, ts, ts),
    ]


def session_dupes(sid: SessionId, ts: str) -> list[CanonicalEvent]:
    return [
        session_started(sid, "backtest", ts),
        session_started(sid, "backtest", ts),
        session_stopped(sid, ts),
        session_stopped(sid, ts),
    ]
