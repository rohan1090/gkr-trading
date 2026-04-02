from __future__ import annotations

from typing import Any

from pydantic import BaseModel

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

SCHEMA_VERSION = 1


class EventEnvelope(BaseModel):
    """Versioned wire format for persistence."""

    model_config = {"frozen": True}

    schema_version: int = SCHEMA_VERSION
    event_type: EventType
    occurred_at_utc: str
    payload: dict[str, Any]


class CanonicalEvent(BaseModel):
    """Typed canonical event (in-memory)."""

    model_config = {"frozen": True}

    schema_version: int = SCHEMA_VERSION
    event_type: EventType
    occurred_at_utc: str
    payload: (
        MarketDataReceivedPayload
        | SignalGeneratedPayload
        | TradeIntentCreatedPayload
        | RiskApprovedPayload
        | RiskRejectedPayload
        | OrderSubmittedPayload
        | OrderAcknowledgedPayload
        | OrderCancelledPayload
        | BrokerOrderRejectedPayload
        | FillReceivedPayload
        | PortfolioUpdatedPayload
        | SessionStartedPayload
        | SessionStoppedPayload
        | ReplayCompletedPayload
    )
