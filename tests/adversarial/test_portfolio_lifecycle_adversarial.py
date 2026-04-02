"""Scenarios 11–20: portfolio and order lifecycle (V1 limits documented)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from gkr_trading.core.events.builders import market_bar, order_submitted, risk_rejected
from gkr_trading.core.events.builders import fill_received as fill_ev
from gkr_trading.core.portfolio import PortfolioState, apply_canonical_event
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, IntentId, OrderId

from tests.adversarial._streams import IID, base_submit_fill


def test_partial_then_final_fill() -> None:
    """11"""
    oid = OrderId("00000000-0000-4000-8000-00000001001")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_ev(oid, IID, OrderSide.BUY, Decimal("3"), Decimal("100"), ts, ts),
        fill_ev(oid, IID, OrderSide.BUY, Decimal("7"), Decimal("101"), ts, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("10")
    assert str(oid) not in s.open_orders
    assert len(s.fill_history) == 2


@pytest.mark.skip(reason="V1: no order_cancelled canonical event (see REMEDIATION.md)")
def test_partial_fill_then_cancel_remaining() -> None:
    """12 — not implementable in V1."""
    assert False


def test_risk_reject_no_position_mutation() -> None:
    """13"""
    from gkr_trading.core.events.builders import trade_intent_created
    from gkr_trading.core.intents.models import TradeIntent
    from gkr_trading.core.schemas.ids import new_intent_id

    ts = "2024-01-01T12:00:00Z"
    intent = TradeIntent(
        intent_id=new_intent_id(),
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("10"),
        order_type=OrderType.MARKET,
        strategy_name="s",
    )
    ev = [
        trade_intent_created(intent, ts),
        risk_rejected(intent.intent_id, "MAX_POSITION", "x", ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions == {}
    assert s.cash == Decimal("10000")
    assert len(s.rejected_actions) == 1


def test_submitted_never_acknowledged_stays_open() -> None:
    """14"""
    oid = OrderId("00000000-0000-4000-8000-00000001002")
    ts = "2024-01-01T12:00:00Z"
    ev = [order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts)]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert str(oid) in s.open_orders
    assert s.open_orders[str(oid)].remaining_qty == Decimal("10")


def test_acknowledged_never_filled_open_untouched() -> None:
    """15 — ack is no-op; order stays open."""
    from gkr_trading.core.events.builders import order_acknowledged

    oid = OrderId("00000000-0000-4000-8000-00000001003")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_acknowledged(oid, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert str(oid) in s.open_orders


def test_oversized_sell_opens_short_remainder() -> None:
    """17 — V1 netting opens short; document as risk."""
    oid_b = OrderId("00000000-0000-4000-8000-00000001004")
    oid_s = OrderId("00000000-0000-4000-8000-00000001005")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid_b, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_ev(oid_b, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
        order_submitted(oid_s, IID, OrderSide.SELL, Decimal("25"), OrderType.MARKET, None, ts),
        fill_ev(oid_s, IID, OrderSide.SELL, Decimal("25"), Decimal("110"), ts, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("-15")
    assert s.realized_pnl == Decimal("100")


def test_multiple_buys_weighted_average() -> None:
    """18"""
    o1 = OrderId("00000000-0000-4000-8000-00000001006")
    o2 = OrderId("00000000-0000-4000-8000-00000001007")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(o1, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_ev(o1, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
        order_submitted(o2, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_ev(o2, IID, OrderSide.BUY, Decimal("10"), Decimal("120"), ts, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("20")
    assert s.avg_entry[str(IID)] == Decimal("110")


def test_mark_update_moves_unrealized_not_realized() -> None:
    """20"""
    oid = OrderId("00000000-0000-4000-8000-00000001008")
    ts = "2024-01-01T12:00:00Z"
    ev = base_submit_fill(oid, ts=ts) + [
        market_bar(
            IID,
            "1d",
            "2024-01-02T12:00:00Z",
            Decimal("1"),
            Decimal("200"),
            Decimal("1"),
            Decimal("150"),
            Decimal("0"),
            "2024-01-02T12:00:00Z",
        )
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.realized_pnl == Decimal("0")
    assert s.unrealized_pnl == Decimal("500")
    assert s.mark_prices[str(IID)] == Decimal("150")


def test_fold_portfolio_updated_overrides_causal_chain() -> None:
    """Applying PORTFOLIO_UPDATED in full fold (not replay) overwrites truth — audit hazard."""
    from gkr_trading.core.events.builders import portfolio_updated

    oid = OrderId("00000000-0000-4000-8000-00000001009")
    ts = "2024-01-01T12:00:00Z"
    ev = base_submit_fill(oid, ts=ts)
    s0 = replay_portfolio_state(ev, Decimal("10000")).state
    fake = PortfolioState.initial(Decimal("99999"))
    pu = portfolio_updated(fake, ts)
    s_bad = PortfolioState.initial(Decimal("10000"))
    for e in ev + [pu]:
        s_bad = apply_canonical_event(s_bad, e)
    assert s_bad.cash == Decimal("99999")
    assert s_bad.positions == {}
    assert replay_portfolio_state(ev + [pu], Decimal("10000")).state == s0
