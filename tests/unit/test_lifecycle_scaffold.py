"""ORDER_CANCELLED / ORDER_REJECTED scaffolding and post-terminal guards."""

from __future__ import annotations

from decimal import Decimal

import pytest

from gkr_trading.core.events.builders import (
    fill_received,
    order_cancelled,
    order_rejected,
    order_submitted,
)
from gkr_trading.core.portfolio import StrictReplayError
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.enums import OrderLifecycleState, OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId

IID = InstrumentId("00000000-0000-4000-8000-00000000lc01")


def test_order_cancelled_closes_open_order() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000lc02")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_cancelled(oid, ts, reason_code="user"),
    ]
    r = replay_portfolio_state(ev, Decimal("100000"))
    assert str(oid) not in r.state.open_orders
    assert r.state.order_lifecycle[str(oid)] == OrderLifecycleState.CANCELED


def test_order_rejected_closes_open_order() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000lc03")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_rejected(oid, "BROKER", ts, reason_detail="halt"),
    ]
    r = replay_portfolio_state(ev, Decimal("100000"))
    assert str(oid) not in r.state.open_orders
    assert r.state.order_lifecycle[str(oid)] == OrderLifecycleState.REJECTED


def test_cancel_after_filled_strict() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000lc04")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts, synthetic_leg_key="f1"
        ),
        order_cancelled(oid, ts),
    ]
    with pytest.raises(StrictReplayError) as e:
        replay_portfolio_state(ev, Decimal("100000"), strict=True)
    assert e.value.code == "CANCEL_AFTER_FILLED"


def test_replay_result_includes_anomalies_by_default() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000lc05")
    ts = "2024-01-01T12:00:00Z"
    ev = [fill_received(oid, IID, OrderSide.BUY, Decimal("1"), Decimal("1"), ts, ts)]
    r = replay_portfolio_state(ev, Decimal("100000"))
    assert len(r.anomalies) >= 1


def test_duplicate_fill_id_still_idempotent() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000lc06")
    ts = "2024-01-01T12:00:00Z"
    fe = fill_received(
        oid, IID, OrderSide.BUY, Decimal("5"), Decimal("50"), ts, ts, synthetic_leg_key="x"
    )
    r = replay_portfolio_state([fe, fe], Decimal("100000"))
    assert len(r.state.fill_history) == 1
    assert len(r.state.applied_fill_ids) == 1
