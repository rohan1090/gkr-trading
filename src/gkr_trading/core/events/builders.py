from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.envelope import CanonicalEvent
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
from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.portfolio.models import PortfolioState
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import (
    FillId,
    InstrumentId,
    IntentId,
    OrderId,
    SessionId,
    resolve_fill_id,
)


def ev(
    event_type: EventType,
    payload,
    occurred_at_utc: str,
) -> CanonicalEvent:
    from gkr_trading.core.events.envelope import SCHEMA_VERSION

    return CanonicalEvent(
        schema_version=SCHEMA_VERSION,
        event_type=event_type,
        occurred_at_utc=occurred_at_utc,
        payload=payload,
    )


def market_bar(
    instrument_id: InstrumentId,
    timeframe: str,
    bar_ts_utc: str,
    o: Decimal,
    h: Decimal,
    l: Decimal,
    c: Decimal,
    v: Decimal,
    occurred_at_utc: str,
) -> CanonicalEvent:
    return ev(
        EventType.MARKET_DATA_RECEIVED,
        MarketDataReceivedPayload(
            instrument_id=instrument_id,
            timeframe=timeframe,
            bar_ts_utc=bar_ts_utc,
            open=o,
            high=h,
            low=l,
            close=c,
            volume=v,
        ),
        occurred_at_utc,
    )


def trade_intent_created(intent: TradeIntent, occurred_at_utc: str) -> CanonicalEvent:
    return ev(
        EventType.TRADE_INTENT_CREATED,
        TradeIntentCreatedPayload(
            intent_id=intent.intent_id,
            instrument_id=intent.instrument_id,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
            limit_price=intent.limit_price,
            strategy_name=intent.strategy_name,
        ),
        occurred_at_utc,
    )


def risk_approved(intent_id: IntentId, order_id: OrderId, occurred_at_utc: str) -> CanonicalEvent:
    return ev(
        EventType.RISK_APPROVED,
        RiskApprovedPayload(intent_id=intent_id, order_id=order_id),
        occurred_at_utc,
    )


def risk_rejected(
    intent_id: IntentId,
    code: str,
    detail: str | None,
    occurred_at_utc: str,
) -> CanonicalEvent:
    return ev(
        EventType.RISK_REJECTED,
        RiskRejectedPayload(intent_id=intent_id, reason_code=code, reason_detail=detail),
        occurred_at_utc,
    )


def order_submitted(
    order_id: OrderId,
    instrument_id: InstrumentId,
    side: OrderSide,
    qty: Decimal,
    order_type: OrderType,
    limit_price: Decimal | None,
    occurred_at_utc: str,
) -> CanonicalEvent:
    return ev(
        EventType.ORDER_SUBMITTED,
        OrderSubmittedPayload(
            order_id=order_id,
            instrument_id=instrument_id,
            side=side,
            quantity=qty,
            order_type=order_type,
            limit_price=limit_price,
        ),
        occurred_at_utc,
    )


def order_acknowledged(
    order_id: OrderId,
    occurred_at_utc: str,
    broker_order_id: str | None = None,
) -> CanonicalEvent:
    return ev(
        EventType.ORDER_ACKNOWLEDGED,
        OrderAcknowledgedPayload(order_id=order_id, broker_order_id=broker_order_id),
        occurred_at_utc,
    )


def fill_received(
    order_id: OrderId,
    instrument_id: InstrumentId,
    side: OrderSide,
    fill_qty: Decimal,
    fill_price: Decimal,
    fill_ts_utc: str,
    occurred_at_utc: str,
    fees: Decimal = Decimal("0"),
    *,
    fill_id: FillId | None = None,
    broker_execution_id: str | None = None,
    dedupe_salt: str = "",
    synthetic_leg_key: str = "default",
) -> CanonicalEvent:
    fid = resolve_fill_id(
        fill_id=fill_id,
        broker_execution_id=broker_execution_id,
        order_id=order_id,
        fill_ts_utc=fill_ts_utc,
        fill_qty=fill_qty,
        fill_price=fill_price,
        fees=fees,
        salt=dedupe_salt,
        synthetic_leg_key=synthetic_leg_key,
    )
    return ev(
        EventType.FILL_RECEIVED,
        FillReceivedPayload(
            order_id=order_id,
            instrument_id=instrument_id,
            side=side,
            fill_qty=fill_qty,
            fill_price=fill_price,
            fees=fees,
            fill_ts_utc=fill_ts_utc,
            fill_id=fid,
            broker_execution_id=broker_execution_id,
            synthetic_leg_key=synthetic_leg_key if not broker_execution_id else None,
        ),
        occurred_at_utc,
    )


def order_cancelled(
    order_id: OrderId,
    occurred_at_utc: str,
    *,
    reason_code: str | None = None,
    cancelled_qty: Decimal | None = None,
) -> CanonicalEvent:
    return ev(
        EventType.ORDER_CANCELLED,
        OrderCancelledPayload(
            order_id=order_id,
            reason_code=reason_code,
            cancelled_qty=cancelled_qty,
        ),
        occurred_at_utc,
    )


def order_rejected(
    order_id: OrderId,
    reason_code: str,
    occurred_at_utc: str,
    *,
    reason_detail: str | None = None,
) -> CanonicalEvent:
    return ev(
        EventType.ORDER_REJECTED,
        BrokerOrderRejectedPayload(
            order_id=order_id,
            reason_code=reason_code,
            reason_detail=reason_detail,
        ),
        occurred_at_utc,
    )


def portfolio_updated(state: PortfolioState, occurred_at_utc: str) -> CanonicalEvent:
    return ev(
        EventType.PORTFOLIO_UPDATED,
        PortfolioUpdatedPayload(
            cash=state.cash,
            positions={k: v for k, v in state.positions.items()},
            avg_cost={k: v for k, v in state.avg_entry.items()},
            realized_pnl=state.realized_pnl,
            unrealized_pnl=state.unrealized_pnl,
            mark_prices={k: v for k, v in state.mark_prices.items()},
        ),
        occurred_at_utc,
    )


def session_started(session_id: SessionId, mode: str, occurred_at_utc: str) -> CanonicalEvent:
    return ev(
        EventType.SESSION_STARTED,
        SessionStartedPayload(session_id=session_id, mode=mode),
        occurred_at_utc,
    )


def session_stopped(session_id: SessionId, occurred_at_utc: str, reason: str | None = None) -> CanonicalEvent:
    return ev(
        EventType.SESSION_STOPPED,
        SessionStoppedPayload(session_id=session_id, reason=reason),
        occurred_at_utc,
    )


def replay_completed(session_id: SessionId, n: int, occurred_at_utc: str) -> CanonicalEvent:
    return ev(
        EventType.REPLAY_COMPLETED,
        ReplayCompletedPayload(session_id=session_id, events_replayed=n),
        occurred_at_utc,
    )


def signal_generated(
    strategy_name: str,
    instrument_id: InstrumentId,
    signal_name: str,
    occurred_at_utc: str,
    strength: Decimal | None = None,
) -> CanonicalEvent:
    return ev(
        EventType.SIGNAL_GENERATED,
        SignalGeneratedPayload(
            strategy_name=strategy_name,
            instrument_id=instrument_id,
            signal_name=signal_name,
            strength=strength,
        ),
        occurred_at_utc,
    )
