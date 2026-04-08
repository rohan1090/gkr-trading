"""Tests for options lifecycle event serialization/deserialization."""
from __future__ import annotations

import pytest

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.payloads import (
    AssignmentReceivedPayload,
    ExerciseProcessedPayload,
    ExpirationProcessedPayload,
    OperatorCommandPayload,
    PendingOrderRegisteredPayload,
    OrderSubmissionAttemptedPayload,
    ReconciliationCompletedPayload,
)
from gkr_trading.core.events.serde import dumps_event, loads_event
from gkr_trading.core.events.types import EventType


class TestOptionsEventSerde:
    def test_assignment_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.ASSIGNMENT_RECEIVED,
            occurred_at_utc="2025-12-19T09:30:00Z",
            payload=AssignmentReceivedPayload(
                event_id="a1",
                instrument_occ_symbol="AAPL251219P00200000",
                instrument_underlying="AAPL",
                venue="alpaca",
                contracts_assigned=2,
                strike_cents=20000,
                right="put",
                resulting_equity_delta=200,
                equity_underlying="AAPL",
                assignment_price_cents=20000,
                effective_date="2025-12-19",
                source="auto",
                requires_operator_review=False,
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.ASSIGNMENT_RECEIVED
        assert restored.payload.contracts_assigned == 2
        assert restored.payload.right == "put"

    def test_exercise_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.EXERCISE_PROCESSED,
            occurred_at_utc="2025-12-19T16:00:00Z",
            payload=ExerciseProcessedPayload(
                event_id="e1",
                instrument_occ_symbol="AAPL251219C00200000",
                instrument_underlying="AAPL",
                venue="alpaca",
                contracts_exercised=3,
                strike_cents=20000,
                right="call",
                resulting_equity_delta=300,
                equity_underlying="AAPL",
                effective_date="2025-12-19",
                initiated_by="system",
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.EXERCISE_PROCESSED
        assert restored.payload.contracts_exercised == 3

    def test_expiration_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.EXPIRATION_PROCESSED,
            occurred_at_utc="2025-12-19T16:00:00Z",
            payload=ExpirationProcessedPayload(
                event_id="x1",
                instrument_occ_symbol="AAPL251219C00200000",
                instrument_underlying="AAPL",
                venue="alpaca",
                contracts_expired=5,
                moneyness_at_expiry="otm",
                premium_paid_cents=25000,
                premium_received_cents=0,
                expiry_type="standard_monthly",
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.EXPIRATION_PROCESSED
        assert restored.payload.contracts_expired == 5

    def test_operator_command_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.OPERATOR_COMMAND,
            occurred_at_utc="2025-12-19T10:00:00Z",
            payload=OperatorCommandPayload(
                command_id="cmd-1",
                command_type="kill_switch",
                parameters='{"level":"full_halt"}',
                operator_id="cli",
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.OPERATOR_COMMAND
        assert restored.payload.command_type == "kill_switch"

    def test_pending_order_registered_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.PENDING_ORDER_REGISTERED,
            occurred_at_utc="2025-12-19T10:00:00Z",
            payload=PendingOrderRegisteredPayload(
                client_order_id="ord-1",
                intent_id="int-1",
                instrument_key="equity:AAPL",
                action="buy_to_open",
                venue="alpaca",
                quantity=100,
                limit_price_cents=15000,
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.PENDING_ORDER_REGISTERED
        assert restored.payload.client_order_id == "ord-1"

    def test_submission_attempted_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.ORDER_SUBMISSION_ATTEMPTED,
            occurred_at_utc="2025-12-19T10:00:01Z",
            payload=OrderSubmissionAttemptedPayload(
                client_order_id="ord-1",
                venue_order_id="V-123",
                success=True,
                rejected=False,
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.ORDER_SUBMISSION_ATTEMPTED
        assert restored.payload.success is True

    def test_reconciliation_completed_roundtrip(self):
        event = CanonicalEvent(
            event_type=EventType.RECONCILIATION_COMPLETED,
            occurred_at_utc="2025-12-19T09:30:00Z",
            payload=ReconciliationCompletedPayload(
                snapshot_id="snap-1",
                trigger="startup",
                status="clean",
                break_count=0,
                blocking_break_count=0,
            ),
        )
        raw = dumps_event(event)
        restored = loads_event(raw)
        assert restored.event_type == EventType.RECONCILIATION_COMPLETED
        assert restored.payload.status == "clean"
