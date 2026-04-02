from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):
    MARKET_DATA_RECEIVED = "market_data_received"
    SIGNAL_GENERATED = "signal_generated"
    TRADE_INTENT_CREATED = "trade_intent_created"
    RISK_APPROVED = "risk_approved"
    RISK_REJECTED = "risk_rejected"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_ACKNOWLEDGED = "order_acknowledged"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REJECTED = "order_rejected"
    FILL_RECEIVED = "fill_received"
    PORTFOLIO_UPDATED = "portfolio_updated"
    SESSION_STARTED = "session_started"
    SESSION_STOPPED = "session_stopped"
    REPLAY_COMPLETED = "replay_completed"
