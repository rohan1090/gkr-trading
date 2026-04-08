"""Tests for OrderStatus transitions and VenueOrder."""
from __future__ import annotations

import pytest

from gkr_trading.core.order_model import OrderStatus, validate_transition


class TestOrderStatusTransitions:
    def test_pending_to_submitted(self):
        assert validate_transition(OrderStatus.PENDING_LOCAL, OrderStatus.SUBMITTED)

    def test_submitted_to_filled(self):
        assert validate_transition(OrderStatus.SUBMITTED, OrderStatus.FILLED)

    def test_filled_is_terminal(self):
        assert OrderStatus.FILLED.is_terminal
        assert not validate_transition(OrderStatus.FILLED, OrderStatus.SUBMITTED)

    def test_canceled_is_terminal(self):
        assert OrderStatus.CANCELED.is_terminal

    def test_rejected_is_terminal(self):
        assert OrderStatus.REJECTED.is_terminal

    def test_unknown_can_resolve_to_filled(self):
        assert validate_transition(OrderStatus.UNKNOWN, OrderStatus.FILLED)

    def test_unknown_can_resolve_to_canceled(self):
        assert validate_transition(OrderStatus.UNKNOWN, OrderStatus.CANCELED)

    def test_pending_can_go_to_unknown(self):
        assert validate_transition(OrderStatus.PENDING_LOCAL, OrderStatus.UNKNOWN)

    def test_submitted_can_go_to_unknown(self):
        assert validate_transition(OrderStatus.SUBMITTED, OrderStatus.UNKNOWN)

    def test_resting_to_filled(self):
        assert validate_transition(OrderStatus.RESTING, OrderStatus.FILLED)

    def test_resting_to_expired(self):
        assert validate_transition(OrderStatus.RESTING, OrderStatus.EXPIRED)

    def test_invalid_transition_rejected(self):
        # Cannot go from FILLED to PENDING_LOCAL
        assert not validate_transition(OrderStatus.FILLED, OrderStatus.PENDING_LOCAL)
        # Cannot go from REJECTED to SUBMITTED
        assert not validate_transition(OrderStatus.REJECTED, OrderStatus.SUBMITTED)
