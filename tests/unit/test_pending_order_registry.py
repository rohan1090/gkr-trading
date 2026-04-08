"""Tests for PendingOrderRegistry — UNKNOWN recovery, duplicate prevention."""
from __future__ import annotations

import sqlite3

import pytest

from gkr_trading.core.order_model import OrderStatus
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry


@pytest.fixture
def registry(tmp_path) -> PendingOrderRegistry:
    conn = sqlite3.connect(str(tmp_path / "orders.db"))
    return PendingOrderRegistry(conn)


class TestDuplicatePrevention:
    def test_first_register_succeeds(self, registry: PendingOrderRegistry):
        assert registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{"asset_class":"equity","ticker":"AAPL"}',
            action="buy_to_open", venue="alpaca", quantity=100,
        )

    def test_duplicate_register_returns_false(self, registry: PendingOrderRegistry):
        """Duplicate client_order_id is rejected (idempotent)."""
        registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{"ticker":"AAPL"}',
            action="buy_to_open", venue="alpaca", quantity=100,
        )
        # Same client_order_id
        assert not registry.register(
            client_order_id="ord-1", intent_id="int-2", session_id="s1",
            instrument_ref_json='{"ticker":"GOOG"}',
            action="sell_to_close", venue="alpaca", quantity=50,
        )

    def test_exists_check(self, registry: PendingOrderRegistry):
        assert not registry.exists("ord-1")
        registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{}', action="buy_to_open",
            venue="alpaca", quantity=100,
        )
        assert registry.exists("ord-1")


class TestStatusTransitions:
    def test_status_update(self, registry: PendingOrderRegistry):
        registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{}', action="buy_to_open",
            venue="alpaca", quantity=100,
        )
        assert registry.get_status("ord-1") == "pending_local"
        registry.update_status("ord-1", OrderStatus.SUBMITTED, venue_order_id="V-123")
        assert registry.get_status("ord-1") == "submitted"
        registry.update_status("ord-1", OrderStatus.FILLED)
        assert registry.get_status("ord-1") == "filled"


class TestUnknownRecovery:
    def test_mark_non_terminal_as_unknown(self, registry: PendingOrderRegistry):
        """On startup: all non-terminal orders become UNKNOWN for reconciliation."""
        # Register several orders in different states
        for i, status in enumerate([
            OrderStatus.PENDING_LOCAL,
            OrderStatus.SUBMITTED,
            OrderStatus.FILLED,     # terminal — should NOT be marked
            OrderStatus.CANCELED,   # terminal — should NOT be marked
        ]):
            registry.register(
                client_order_id=f"ord-{i}", intent_id=f"int-{i}", session_id="s1",
                instrument_ref_json='{}', action="buy_to_open",
                venue="alpaca", quantity=100,
            )
            if status != OrderStatus.PENDING_LOCAL:
                registry.update_status(f"ord-{i}", status)

        # Simulate restart: mark all non-terminal as UNKNOWN
        count = registry.mark_all_non_terminal_as_unknown("s1")
        assert count == 2  # PENDING_LOCAL and SUBMITTED

        # Verify
        assert registry.get_status("ord-0") == "unknown"
        assert registry.get_status("ord-1") == "unknown"
        assert registry.get_status("ord-2") == "filled"    # unchanged
        assert registry.get_status("ord-3") == "canceled"   # unchanged

    def test_get_unknown_orders(self, registry: PendingOrderRegistry):
        registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{"ticker":"AAPL"}',
            action="buy_to_open", venue="alpaca", quantity=100,
        )
        registry.update_status("ord-1", OrderStatus.UNKNOWN)
        unknowns = registry.get_unknown_orders("s1")
        assert len(unknowns) == 1
        assert unknowns[0]["client_order_id"] == "ord-1"

    def test_no_resubmission_without_reconciliation(self, registry: PendingOrderRegistry):
        """UNKNOWN orders must be reconciled before resubmission.

        This test verifies that UNKNOWN orders are visible via get_unknown_orders
        and must be explicitly resolved.
        """
        registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{}', action="buy_to_open",
            venue="alpaca", quantity=100,
        )
        registry.mark_all_non_terminal_as_unknown("s1")

        # UNKNOWN orders are visible
        unknowns = registry.get_unknown_orders("s1")
        assert len(unknowns) == 1

        # Cannot register a new order with same client_order_id
        assert not registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{}', action="buy_to_open",
            venue="alpaca", quantity=100,
        )

        # Must explicitly resolve the UNKNOWN order
        registry.update_status("ord-1", OrderStatus.CANCELED)
        assert registry.get_status("ord-1") == "canceled"
        unknowns = registry.get_unknown_orders("s1")
        assert len(unknowns) == 0


class TestActiveOrders:
    def test_get_active_excludes_terminal(self, registry: PendingOrderRegistry):
        registry.register(
            client_order_id="ord-1", intent_id="int-1", session_id="s1",
            instrument_ref_json='{}', action="buy_to_open",
            venue="alpaca", quantity=100,
        )
        registry.register(
            client_order_id="ord-2", intent_id="int-2", session_id="s1",
            instrument_ref_json='{}', action="sell_to_close",
            venue="alpaca", quantity=50,
        )
        registry.update_status("ord-2", OrderStatus.FILLED)

        active = registry.get_active_orders("s1")
        assert len(active) == 1
        assert active[0]["client_order_id"] == "ord-1"
