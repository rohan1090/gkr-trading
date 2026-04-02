"""Scenarios 21–25: risk invariants (complements unit/test_risk.py)."""

from __future__ import annotations

from datetime import UTC, datetime, time
from decimal import Decimal

from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.portfolio.models import OpenOrder, PortfolioState
from gkr_trading.core.risk import RiskLimits, evaluate_intent
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.fixed_clock import FixedClock
from gkr_trading.core.schemas.ids import InstrumentId, IntentId, OrderId

from tests.adversarial._streams import IID


def _limits() -> RiskLimits:
    return RiskLimits(
        max_position_abs=Decimal("50"),
        max_notional_per_trade=Decimal("5000"),
        session_start_utc=time(9, 0),
        session_end_utc=time(17, 0),
        kill_switch=False,
    )


def _intent(qty: Decimal = Decimal("5")) -> TradeIntent:
    return TradeIntent(
        intent_id=IntentId("00000000-0000-4000-8000-00000002001"),
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=qty,
        order_type=OrderType.MARKET,
        strategy_name="s",
    )


def test_max_position_breach_21() -> None:
    lim = _limits()
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    st = PortfolioState.initial(Decimal("100000"))
    st.positions[str(IID)] = Decimal("48")
    st.avg_entry[str(IID)] = Decimal("10")
    r = evaluate_intent(_intent(Decimal("5")), st, lim, clock, Decimal("10"), OrderId("o"))
    assert not r.approved and r.reason_code == "MAX_POSITION"


def test_max_notional_breach_22() -> None:
    lim = _limits()
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    r = evaluate_intent(_intent(Decimal("1000")), PortfolioState.initial(Decimal("100000")), lim, clock, Decimal("10"), OrderId("o"))
    assert not r.approved and r.reason_code == "MAX_NOTIONAL"


def test_kill_switch_23() -> None:
    lim = RiskLimits(
        Decimal("1000"),
        Decimal("100000"),
        time(0, 0),
        time(23, 59, 59),
        kill_switch=True,
    )
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    r = evaluate_intent(_intent(), PortfolioState.initial(Decimal("100000")), lim, clock, Decimal("10"), OrderId("o"))
    assert not r.approved and r.reason_code == "KILL_SWITCH"


def test_session_closed_24() -> None:
    lim = _limits()
    clock = FixedClock(datetime(2024, 1, 2, 8, 0, tzinfo=UTC))
    r = evaluate_intent(_intent(), PortfolioState.initial(Decimal("100000")), lim, clock, Decimal("10"), OrderId("o"))
    assert not r.approved and r.reason_code == "OUTSIDE_SESSION"


def test_repeated_intent_blocked_by_open_order_25() -> None:
    lim = _limits()
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    oid = OrderId("00000000-0000-4000-8000-00000002002")
    st = PortfolioState.initial(Decimal("100000"))
    st.open_orders[str(oid)] = OpenOrder(
        order_id=oid,
        instrument_id=IID,
        side=OrderSide.BUY,
        remaining_qty=Decimal("5"),
        initial_quantity=Decimal("10"),
        order_type=OrderType.MARKET,
    )
    r = evaluate_intent(_intent(), st, lim, clock, Decimal("10"), OrderId("o2"))
    assert not r.approved and r.reason_code == "DUPLICATE_OPEN_ORDER"
