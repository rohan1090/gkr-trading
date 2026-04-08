from __future__ import annotations

import json
from typing import Any, Type

from gkr_trading.core.events.envelope import SCHEMA_VERSION, CanonicalEvent, EventEnvelope
from gkr_trading.core.events.payloads import (
    AssignmentReceivedPayload,
    BrokerOrderRejectedPayload,
    ExerciseProcessedPayload,
    ExpirationProcessedPayload,
    FillReceivedPayload,
    MarketDataReceivedPayload,
    OperatorCommandPayload,
    OrderAcknowledgedPayload,
    OrderCancelledPayload,
    OrderSubmissionAttemptedPayload,
    OrderSubmittedPayload,
    PendingOrderRegisteredPayload,
    PortfolioUpdatedPayload,
    ReconciliationCompletedPayload,
    ReplayCompletedPayload,
    RiskApprovedPayload,
    RiskRejectedPayload,
    SessionStartedPayload,
    SessionStoppedPayload,
    SignalGeneratedPayload,
    TradeIntentCreatedPayload,
)
from gkr_trading.core.events.types import EventType

_PAYLOAD_MAP: dict[EventType, Type[Any]] = {
    EventType.MARKET_DATA_RECEIVED: MarketDataReceivedPayload,
    EventType.SIGNAL_GENERATED: SignalGeneratedPayload,
    EventType.TRADE_INTENT_CREATED: TradeIntentCreatedPayload,
    EventType.RISK_APPROVED: RiskApprovedPayload,
    EventType.RISK_REJECTED: RiskRejectedPayload,
    EventType.ORDER_SUBMITTED: OrderSubmittedPayload,
    EventType.ORDER_ACKNOWLEDGED: OrderAcknowledgedPayload,
    EventType.ORDER_CANCELLED: OrderCancelledPayload,
    EventType.ORDER_REJECTED: BrokerOrderRejectedPayload,
    EventType.FILL_RECEIVED: FillReceivedPayload,
    EventType.PORTFOLIO_UPDATED: PortfolioUpdatedPayload,
    EventType.SESSION_STARTED: SessionStartedPayload,
    EventType.SESSION_STOPPED: SessionStoppedPayload,
    EventType.REPLAY_COMPLETED: ReplayCompletedPayload,
    # Options lifecycle
    EventType.ASSIGNMENT_RECEIVED: AssignmentReceivedPayload,
    EventType.EXERCISE_PROCESSED: ExerciseProcessedPayload,
    EventType.EXPIRATION_PROCESSED: ExpirationProcessedPayload,
    # Control-plane
    EventType.OPERATOR_COMMAND: OperatorCommandPayload,
    EventType.RECONCILIATION_COMPLETED: ReconciliationCompletedPayload,
    EventType.PENDING_ORDER_REGISTERED: PendingOrderRegisteredPayload,
    EventType.ORDER_SUBMISSION_ATTEMPTED: OrderSubmissionAttemptedPayload,
}


def payload_model_for(event_type: EventType) -> Type[Any]:
    return _PAYLOAD_MAP[event_type]


def canonical_to_envelope(ev: CanonicalEvent) -> EventEnvelope:
    return EventEnvelope(
        schema_version=ev.schema_version,
        event_type=ev.event_type,
        occurred_at_utc=ev.occurred_at_utc,
        payload=json.loads(ev.payload.model_dump_json()),
    )


def envelope_to_canonical(env: EventEnvelope) -> CanonicalEvent:
    model = payload_model_for(env.event_type)
    payload = model.model_validate(env.payload)
    return CanonicalEvent(
        schema_version=env.schema_version,
        event_type=env.event_type,
        occurred_at_utc=env.occurred_at_utc,
        payload=payload,
    )


def dumps_event(ev: CanonicalEvent) -> str:
    env = canonical_to_envelope(ev)
    return env.model_dump_json()


def loads_event(raw: str) -> CanonicalEvent:
    env = EventEnvelope.model_validate_json(raw)
    if env.schema_version != SCHEMA_VERSION:
        raise ValueError(f"Unsupported schema_version: {env.schema_version}")
    return envelope_to_canonical(env)
