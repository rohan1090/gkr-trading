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
    # Options lifecycle events
    ASSIGNMENT_RECEIVED = "assignment_received"
    EXERCISE_PROCESSED = "exercise_processed"
    EXPIRATION_PROCESSED = "expiration_processed"
    # Options-aware order events
    OPTIONS_ORDER_SUBMITTED = "options_order_submitted"
    # Operator / control-plane events
    OPERATOR_COMMAND = "operator_command"
    RECONCILIATION_COMPLETED = "reconciliation_completed"
    PENDING_ORDER_REGISTERED = "pending_order_registered"
    ORDER_SUBMISSION_ATTEMPTED = "order_submission_attempted"
