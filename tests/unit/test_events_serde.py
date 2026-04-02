from __future__ import annotations

from decimal import Decimal

import pytest

from gkr_trading.core.events import CanonicalEvent, dumps_event, loads_event
from gkr_trading.core.events.envelope import SCHEMA_VERSION
from gkr_trading.core.events.payloads import (
    BrokerOrderRejectedPayload,
    FillReceivedPayload,
    MarketDataReceivedPayload,
    OrderAcknowledgedPayload,
    OrderCancelledPayload,
    OrderSubmittedPayload,
    PortfolioUpdatedPayload,
    ReplayCompletedPayload,
    RiskApprovedPayload,
    RiskRejectedPayload,
    SessionStartedPayload,
    SessionStoppedPayload,
    SignalGeneratedPayload,
    TradeIntentCreatedPayload,
)
from gkr_trading.core.events.types import EventType
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import FillId, InstrumentId, IntentId, OrderId, SessionId


def _wrap(et: EventType, payload, ts: str = "2024-01-01T12:00:00Z") -> CanonicalEvent:
    return CanonicalEvent(
        schema_version=SCHEMA_VERSION,
        event_type=et,
        occurred_at_utc=ts,
        payload=payload,
    )


@pytest.mark.parametrize(
    "event",
    [
        _wrap(
            EventType.MARKET_DATA_RECEIVED,
            MarketDataReceivedPayload(
                instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
                timeframe="1d",
                bar_ts_utc="2024-01-01T00:00:00Z",
                open=Decimal("1"),
                high=Decimal("2"),
                low=Decimal("0.5"),
                close=Decimal("1.5"),
                volume=Decimal("100"),
            ),
        ),
        _wrap(
            EventType.SIGNAL_GENERATED,
            SignalGeneratedPayload(
                strategy_name="s",
                instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
                signal_name="x",
                strength=Decimal("0.5"),
            ),
        ),
        _wrap(
            EventType.TRADE_INTENT_CREATED,
            TradeIntentCreatedPayload(
                intent_id=IntentId("00000000-0000-4000-8000-000000000011"),
                instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
                side=OrderSide.BUY,
                quantity=Decimal("10"),
                order_type=OrderType.MARKET,
                limit_price=None,
                strategy_name="s",
            ),
        ),
        _wrap(
            EventType.RISK_APPROVED,
            RiskApprovedPayload(
                intent_id=IntentId("00000000-0000-4000-8000-000000000011"),
                order_id=OrderId("00000000-0000-4000-8000-000000000022"),
            ),
        ),
        _wrap(
            EventType.RISK_REJECTED,
            RiskRejectedPayload(
                intent_id=IntentId("00000000-0000-4000-8000-000000000011"),
                reason_code="X",
                reason_detail="d",
            ),
        ),
        _wrap(
            EventType.ORDER_SUBMITTED,
            OrderSubmittedPayload(
                order_id=OrderId("00000000-0000-4000-8000-000000000022"),
                instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
                side=OrderSide.SELL,
                quantity=Decimal("3"),
                order_type=OrderType.LIMIT,
                limit_price=Decimal("10"),
            ),
        ),
        _wrap(
            EventType.ORDER_ACKNOWLEDGED,
            OrderAcknowledgedPayload(
                order_id=OrderId("00000000-0000-4000-8000-000000000022"),
                broker_order_id="b1",
            ),
        ),
        _wrap(
            EventType.FILL_RECEIVED,
            FillReceivedPayload(
                order_id=OrderId("00000000-0000-4000-8000-000000000022"),
                instrument_id=InstrumentId("00000000-0000-4000-8000-000000000001"),
                side=OrderSide.BUY,
                fill_qty=Decimal("3"),
                fill_price=Decimal("9.5"),
                fees=Decimal("0.01"),
                fill_ts_utc="2024-01-01T12:00:01Z",
                fill_id=FillId("exec:brk_roundtrip_1"),
                broker_execution_id="brk_roundtrip_1",
            ),
        ),
        _wrap(
            EventType.ORDER_CANCELLED,
            OrderCancelledPayload(
                order_id=OrderId("00000000-0000-4000-8000-000000000022"),
                reason_code="USER",
            ),
        ),
        _wrap(
            EventType.ORDER_REJECTED,
            BrokerOrderRejectedPayload(
                order_id=OrderId("00000000-0000-4000-8000-000000000022"),
                reason_code="BROKER",
                reason_detail="insufficient margin",
            ),
        ),
        _wrap(
            EventType.PORTFOLIO_UPDATED,
            PortfolioUpdatedPayload(
                cash=Decimal("100"),
                positions={"00000000-0000-4000-8000-000000000001": Decimal("3")},
                avg_cost={"00000000-0000-4000-8000-000000000001": Decimal("9.5")},
                realized_pnl=Decimal("0"),
                unrealized_pnl=Decimal("1"),
                mark_prices={"00000000-0000-4000-8000-000000000001": Decimal("10")},
            ),
        ),
        _wrap(
            EventType.SESSION_STARTED,
            SessionStartedPayload(session_id=SessionId("00000000-0000-4000-8000-000000000099"), mode="paper"),
        ),
        _wrap(
            EventType.SESSION_STOPPED,
            SessionStoppedPayload(session_id=SessionId("00000000-0000-4000-8000-000000000099"), reason="done"),
        ),
        _wrap(
            EventType.REPLAY_COMPLETED,
            ReplayCompletedPayload(session_id=SessionId("00000000-0000-4000-8000-000000000099"), events_replayed=42),
        ),
    ],
)
def test_roundtrip_event(event: CanonicalEvent) -> None:
    raw = dumps_event(event)
    got = loads_event(raw)
    assert got.event_type == event.event_type
    assert got.payload == event.payload
