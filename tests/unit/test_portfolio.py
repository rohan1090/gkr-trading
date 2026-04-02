from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.builders import order_submitted
from gkr_trading.core.events.builders import fill_received as fill_ev
from gkr_trading.core.portfolio import PortfolioState, apply_canonical_event
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId


def test_buy_fill_updates_cash_and_position() -> None:
    iid = InstrumentId("00000000-0000-4000-8000-000000000001")
    oid = OrderId("00000000-0000-4000-8000-000000000022")
    s0 = PortfolioState.initial(Decimal("1000"))
    os_ = order_submitted(oid, iid, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, "t")
    s1 = apply_canonical_event(s0, os_)
    assert str(oid) in s1.open_orders
    fl = fill_ev(oid, iid, OrderSide.BUY, Decimal("10"), Decimal("100"), "t", "t")
    s2 = apply_canonical_event(s1, fl)
    assert s2.cash == Decimal("0")
    assert s2.positions[str(iid)] == Decimal("10")
    assert s2.avg_entry[str(iid)] == Decimal("100")
    assert len(s2.fill_history) == 1


def test_sell_realizes_pnl() -> None:
    iid = InstrumentId("00000000-0000-4000-8000-000000000001")
    oid_b = OrderId("00000000-0000-4000-8000-000000000001")
    oid_s = OrderId("00000000-0000-4000-8000-000000000002")
    s = PortfolioState.initial(Decimal("1000"))
    s = apply_canonical_event(s, order_submitted(oid_b, iid, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, "t"))
    s = apply_canonical_event(s, fill_ev(oid_b, iid, OrderSide.BUY, Decimal("10"), Decimal("100"), "t", "t"))
    s = apply_canonical_event(s, order_submitted(oid_s, iid, OrderSide.SELL, Decimal("10"), OrderType.MARKET, None, "t"))
    s = apply_canonical_event(s, fill_ev(oid_s, iid, OrderSide.SELL, Decimal("10"), Decimal("110"), "t", "t"))
    assert s.positions.get(str(iid)) is None
    assert s.realized_pnl == Decimal("100")
    assert s.cash == Decimal("1100")
