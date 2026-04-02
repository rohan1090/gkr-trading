from __future__ import annotations

from datetime import UTC, datetime, time
from decimal import Decimal

import pytest

from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.portfolio import PortfolioState
from gkr_trading.core.risk import RiskEngine, RiskLimits, evaluate_intent
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.fixed_clock import FixedClock
from gkr_trading.core.schemas.ids import InstrumentId, IntentId, OrderId


@pytest.fixture
def limits() -> RiskLimits:
    return RiskLimits(
        max_position_abs=Decimal("100"),
        max_notional_per_trade=Decimal("1000"),
        session_start_utc=time(9, 30),
        session_end_utc=time(16, 0),
        kill_switch=False,
    )


def _intent(qty: Decimal = Decimal("10")) -> TradeIntent:
    return TradeIntent(
        intent_id=IntentId("00000000-0000-4000-8000-000000000011"),
        instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
        side=OrderSide.BUY,
        quantity=qty,
        order_type=OrderType.MARKET,
        strategy_name="t",
    )


def test_kill_switch(limits: RiskLimits) -> None:
    lim = RiskLimits(
        limits.max_position_abs,
        limits.max_notional_per_trade,
        limits.session_start_utc,
        limits.session_end_utc,
        kill_switch=True,
    )
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    r = evaluate_intent(_intent(), PortfolioState.initial(Decimal("10000")), lim, clock, Decimal("50"), OrderId("o"))
    assert not r.approved
    assert r.reason_code == "KILL_SWITCH"


def test_outside_session(limits: RiskLimits) -> None:
    clock = FixedClock(datetime(2024, 1, 2, 8, 0, tzinfo=UTC))
    r = evaluate_intent(_intent(), PortfolioState.initial(Decimal("10000")), limits, clock, Decimal("50"), OrderId("o"))
    assert not r.approved
    assert r.reason_code == "OUTSIDE_SESSION"


def test_max_notional(limits: RiskLimits) -> None:
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    r = evaluate_intent(
        _intent(Decimal("100")),
        PortfolioState.initial(Decimal("10000")),
        limits,
        clock,
        Decimal("20"),
        OrderId("o"),
    )
    assert not r.approved
    assert r.reason_code == "MAX_NOTIONAL"


def test_max_position(limits: RiskLimits) -> None:
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    st = PortfolioState.initial(Decimal("10000"))
    st.positions["00000000-0000-4000-8000-000000000001"] = Decimal("95")
    st.avg_entry["00000000-0000-4000-8000-000000000001"] = Decimal("10")
    r = evaluate_intent(_intent(Decimal("10")), st, limits, clock, Decimal("10"), OrderId("o"))
    assert not r.approved
    assert r.reason_code == "MAX_POSITION"


def test_duplicate_open_order(limits: RiskLimits) -> None:
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    from gkr_trading.core.portfolio.models import OpenOrder

    oid = OrderId("00000000-0000-4000-8000-000000000099")
    st = PortfolioState.initial(Decimal("10000"))
    st.open_orders[str(oid)] = OpenOrder(
        order_id=oid,
        instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
        side=OrderSide.BUY,
        remaining_qty=Decimal("5"),
        initial_quantity=Decimal("10"),
        order_type=OrderType.MARKET,
    )
    r = evaluate_intent(_intent(Decimal("5")), st, limits, clock, Decimal("10"), OrderId("o2"))
    assert not r.approved
    assert r.reason_code == "DUPLICATE_OPEN_ORDER"


def test_approved(limits: RiskLimits) -> None:
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    oid = OrderId("00000000-0000-4000-8000-000000000033")
    r = evaluate_intent(_intent(), PortfolioState.initial(Decimal("10000")), limits, clock, Decimal("10"), oid)
    assert r.approved
    assert r.order_id == oid


def test_risk_engine_wrapper(limits: RiskLimits) -> None:
    eng = RiskEngine(limits)
    clock = FixedClock(datetime(2024, 1, 2, 10, 0, tzinfo=UTC))
    oid = OrderId("00000000-0000-4000-8000-000000000033")
    r = eng.evaluate(_intent(), PortfolioState.initial(Decimal("10000")), clock, Decimal("10"), oid)
    assert r.approved
