"""Fill idempotency, order lifecycle enforcement, replay strict mode, and anomalies."""

from __future__ import annotations

from decimal import Decimal

import pytest

from gkr_trading.core.events.builders import (
    fill_received,
    order_acknowledged,
    order_submitted,
    portfolio_updated,
)
from gkr_trading.core.portfolio import (
    PortfolioAnomaly,
    PortfolioState,
    StrictReplayError,
    apply_canonical_event,
)
from gkr_trading.core.portfolio.models import OpenOrder
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.enums import OrderLifecycleState, OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId

IID = InstrumentId("00000000-0000-4000-8000-00000000cc01")


def test_fill_before_ack_same_as_ack_before_fill() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc10")
    ts = "2024-01-01T12:00:00Z"
    ev_a = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_acknowledged(oid, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    ev_b = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
        order_acknowledged(oid, ts),
    ]
    assert replay_portfolio_state(ev_a, Decimal("100000")).state == replay_portfolio_state(
        ev_b, Decimal("100000")
    ).state


def test_partial_then_filled_lifecycle() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc11")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("4"), Decimal("100"), ts, ts, dedupe_salt="p1"
        ),
    ]
    s = replay_portfolio_state(ev, Decimal("100000")).state
    assert s.open_orders[str(oid)].lifecycle == OrderLifecycleState.PARTIALLY_FILLED
    assert s.open_orders[str(oid)].remaining_qty == Decimal("6")
    ev2 = ev + [
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("6"), Decimal("100"), ts, ts, dedupe_salt="p2"
        )
    ]
    s2 = replay_portfolio_state(ev2, Decimal("100000")).state
    assert str(oid) not in s2.open_orders
    assert s2.order_lifecycle[str(oid)] == OrderLifecycleState.FILLED


def test_strict_replay_rejects_orphan_fill() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc12")
    ts = "2024-01-01T12:00:00Z"
    ev = [fill_received(oid, IID, OrderSide.BUY, Decimal("5"), Decimal("50"), ts, ts)]
    with pytest.raises(StrictReplayError) as exc:
        replay_portfolio_state(ev, Decimal("10000"), strict=True)
    assert exc.value.code == "ORPHAN_FILL"


def test_permissive_replay_orphan_fill_emits_anomaly() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc13")
    ts = "2024-01-01T12:00:00Z"
    ev = [fill_received(oid, IID, OrderSide.BUY, Decimal("5"), Decimal("50"), ts, ts)]
    anomalies: list[PortfolioAnomaly] = []
    s = replay_portfolio_state(ev, Decimal("10000"), anomalies=anomalies).state
    assert s.positions[str(IID)] == Decimal("5")
    assert len(anomalies) == 1
    assert anomalies[0].code == "ORPHAN_FILL"


def test_fill_exceeds_remaining_strict() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc14")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("11"), Decimal("100"), ts, ts, dedupe_salt="x"
        ),
    ]
    with pytest.raises(StrictReplayError) as exc:
        replay_portfolio_state(ev, Decimal("100000"), strict=True)
    assert exc.value.code == "FILL_VIOLATES_OPEN_ORDER"


def test_fill_exceeds_remaining_permissive_no_position_change() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc15")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("11"), Decimal("100"), ts, ts, dedupe_salt="x"
        ),
    ]
    anomalies: list[PortfolioAnomaly] = []
    s = replay_portfolio_state(ev, Decimal("100000"), anomalies=anomalies).state
    assert str(IID) not in s.positions
    assert len(s.fill_history) == 0
    assert len(anomalies) == 1


def test_duplicate_submit_after_filled_strict() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc16")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
    ]
    with pytest.raises(StrictReplayError) as exc:
        replay_portfolio_state(ev, Decimal("100000"), strict=True)
    assert exc.value.code == "DUPLICATE_SUBMIT_AFTER_TERMINAL"


def test_invalid_ack_transition_strict() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc17")
    ts = "2024-01-01T12:00:00Z"
    s = PortfolioState.initial(Decimal("100000"))
    key = str(oid)
    s.open_orders[key] = OpenOrder(
        order_id=oid,
        instrument_id=IID,
        side=OrderSide.BUY,
        remaining_qty=Decimal("10"),
        initial_quantity=Decimal("10"),
        order_type=OrderType.MARKET,
        lifecycle=OrderLifecycleState.NEW,
    )
    s.order_lifecycle[key] = OrderLifecycleState.NEW
    with pytest.raises(StrictReplayError) as exc:
        apply_canonical_event(state=s, event=order_acknowledged(oid, ts), strict=True)
    assert exc.value.code == "INVALID_ACK_TRANSITION"


def test_replay_twice_bitwise_identical() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc18")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    a = replay_portfolio_state(ev, Decimal("100000")).state
    b = replay_portfolio_state(ev, Decimal("100000")).state
    assert a == b


def test_strip_portfolio_updated_replay_truth_unchanged() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000cc19")
    ts = "2024-01-01T12:00:00Z"
    core = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    st = replay_portfolio_state(core, Decimal("100000")).state
    fake = PortfolioState.initial(Decimal("99999"))
    pu = portfolio_updated(fake, ts)
    assert replay_portfolio_state(core + [pu, pu], Decimal("100000")).state == st
