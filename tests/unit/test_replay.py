from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.builders import (
    market_bar,
    order_submitted,
    risk_approved,
    trade_intent_created,
)
from gkr_trading.core.events.builders import fill_received as fill_ev
from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, IntentId, OrderId


def test_replay_determinism() -> None:
    iid = InstrumentId("00000000-0000-4000-8000-000000000001")
    oid = OrderId("00000000-0000-4000-8000-000000000022")
    ts = "2024-01-01T12:00:00Z"
    events = [
        market_bar(iid, "1d", ts, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("10"), Decimal("0"), ts),
        trade_intent_created(
            TradeIntent(
                intent_id=IntentId("00000000-0000-4000-8000-000000000011"),
                instrument_id=iid,
                side=OrderSide.BUY,
                quantity=Decimal("2"),
                order_type=OrderType.MARKET,
                strategy_name="s",
            ),
            ts,
        ),
        risk_approved(IntentId("00000000-0000-4000-8000-000000000011"), oid, ts),
        order_submitted(oid, iid, OrderSide.BUY, Decimal("2"), OrderType.MARKET, None, ts),
        fill_ev(oid, iid, OrderSide.BUY, Decimal("2"), Decimal("10"), ts, ts),
    ]
    a = replay_portfolio_state(events, Decimal("100")).state
    b = replay_portfolio_state(events, Decimal("100")).state
    assert a.cash == b.cash and a.positions == b.positions and a.realized_pnl == b.realized_pnl
    assert a.positions.get(str(iid)) == Decimal("2")
