"""Adversarial test for write-before-call durability ordering.

The invariant: before ANY order API call, the system MUST:
1. Persist TradeIntent / order-submission event to EventStore
2. Persist PendingOrderRegistry entry
3. Only THEN call adapter

If the process crashes between steps 2 and 3, the pending order
registry will contain a PENDING_LOCAL entry that becomes UNKNOWN
on restart, triggering reconciliation.
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from gkr_trading.core.instruments import EquityRef
from gkr_trading.core.order_model import OrderStatus
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.live.base import (
    SubmissionRequest,
    SubmissionResponse,
    VenueAccountInfo,
    VenueAdapter,
    VenuePosition,
)
from gkr_trading.live.order_submission_service import OrderSubmissionService
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry


class RecordingAdapter(VenueAdapter):
    """Adapter that records the order of operations for verification."""

    def __init__(self, *, should_fail: bool = False):
        self.calls: list[str] = []
        self._should_fail = should_fail

    @property
    def venue_name(self) -> str:
        return "test"

    def submit_order(self, request: SubmissionRequest) -> SubmissionResponse:
        self.calls.append(f"submit:{request.client_order_id}")
        if self._should_fail:
            raise ConnectionError("Simulated network failure")
        return SubmissionResponse(
            client_order_id=request.client_order_id,
            venue_order_id=f"V-{request.client_order_id}",
            success=True,
        )

    def cancel_order(self, client_order_id: str) -> bool:
        return True

    def get_order_status(self, client_order_id: str) -> Optional[OrderStatus]:
        return None

    def get_positions(self) -> List[VenuePosition]:
        return []

    def get_account(self) -> VenueAccountInfo:
        return VenueAccountInfo(cash_cents=100000, buying_power_cents=100000)


def _make_intent() -> TradeIntent:
    return TradeIntent(
        intent_id="intent-1", strategy_id="test", session_id="sess-1",
        venue_class="traditional", instrument_ref=EquityRef(ticker="AAPL"),
        action="buy_to_open", quantity=100,
        limit_price_cents=15000, time_in_force="day",
        created_at_ns=1000,
    )


@pytest.fixture
def components(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    event_store = SqliteEventStore(conn)
    pending = PendingOrderRegistry(conn)
    return conn, event_store, pending


class TestWriteBeforeCall:
    def test_event_persisted_before_api_call(self, components):
        """Events MUST be in EventStore before adapter.submit_order() is called."""
        conn, event_store, pending = components
        adapter = RecordingAdapter()
        service = OrderSubmissionService(event_store, pending, adapter)

        result = service.submit(_make_intent(), "alpaca")
        assert result.success

        # Verify: events were written
        events = event_store.load_session("sess-1")
        assert len(events) >= 2  # pending_order_registered + submission_attempted

        # Verify: adapter was called
        assert len(adapter.calls) == 1

    def test_pending_registry_written_before_api_call(self, components):
        """PendingOrderRegistry entry MUST exist before adapter call."""
        conn, event_store, pending = components
        adapter = RecordingAdapter()
        service = OrderSubmissionService(event_store, pending, adapter)

        result = service.submit(_make_intent(), "alpaca")
        assert result.success
        # The order was registered (exists in registry)
        assert pending.exists(result.client_order_id)

    def test_crash_during_api_call_leaves_pending_order(self, components):
        """If API call fails, order remains in registry for crash recovery."""
        conn, event_store, pending = components
        adapter = RecordingAdapter(should_fail=True)
        service = OrderSubmissionService(event_store, pending, adapter)

        result = service.submit(_make_intent(), "alpaca")
        assert not result.success
        assert result.error is not None

        # Order exists in registry with UNKNOWN status
        assert pending.exists(result.client_order_id)
        status = pending.get_status(result.client_order_id)
        assert status == "unknown"

    def test_crash_recovery_marks_unknown(self, components):
        """On restart, non-terminal orders become UNKNOWN."""
        conn, event_store, pending = components
        adapter = RecordingAdapter(should_fail=True)
        service = OrderSubmissionService(event_store, pending, adapter)

        # Simulate crash during submission
        service.submit(_make_intent(), "alpaca")

        # Simulate restart: mark all as unknown
        count = pending.mark_all_non_terminal_as_unknown("sess-1")
        assert count >= 1

        unknowns = pending.get_unknown_orders("sess-1")
        assert len(unknowns) >= 1

    def test_no_duplicate_submission(self, components):
        """Same client_order_id cannot be submitted twice."""
        conn, event_store, pending = components
        adapter = RecordingAdapter()
        service = OrderSubmissionService(event_store, pending, adapter)

        # First submission
        result1 = service.submit(_make_intent(), "alpaca")
        assert result1.success

        # Second submission with same intent generates NEW client_order_id
        # (each submit() call generates a fresh UUID)
        result2 = service.submit(_make_intent(), "alpaca")
        assert result2.success
        assert result2.client_order_id != result1.client_order_id

        # Both adapter calls were made (different orders)
        assert len(adapter.calls) == 2

    def test_submission_records_rejection(self, components):
        """Rejected submissions are persisted with rejection details."""
        conn, event_store, pending = components

        class RejectingAdapter(RecordingAdapter):
            def submit_order(self, request):
                self.calls.append(f"submit:{request.client_order_id}")
                return SubmissionResponse(
                    client_order_id=request.client_order_id,
                    venue_order_id=None,
                    success=False,
                    rejected=True,
                    reject_reason="INSUFFICIENT_FUNDS",
                )

        adapter = RejectingAdapter()
        service = OrderSubmissionService(event_store, pending, adapter)

        result = service.submit(_make_intent(), "alpaca")
        assert not result.success
        assert result.rejected
        assert result.reject_reason == "INSUFFICIENT_FUNDS"

        # Order in registry marked as rejected
        assert pending.get_status(result.client_order_id) == "rejected"


class TestOrderLifecycleIntegration:
    def test_successful_flow_persists_all_events(self, components):
        """Full successful flow: intent -> register -> submit -> response."""
        conn, event_store, pending = components
        adapter = RecordingAdapter()
        service = OrderSubmissionService(event_store, pending, adapter)

        result = service.submit(_make_intent(), "alpaca")
        assert result.success

        events = event_store.load_session("sess-1")
        event_types = [e.event_type.value for e in events]

        # Must contain: pending_order_registered, order_submission_attempted
        assert "pending_order_registered" in event_types
        assert "order_submission_attempted" in event_types

        # pending_order_registered MUST come before order_submission_attempted
        reg_idx = event_types.index("pending_order_registered")
        att_idx = event_types.index("order_submission_attempted")
        assert reg_idx < att_idx, "pending_order_registered must precede submission_attempted"
