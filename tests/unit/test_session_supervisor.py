"""Tests for SessionSupervisor — startup reconciliation, kill switches."""
from __future__ import annotations

import sqlite3
from typing import List, Optional

import pytest

from gkr_trading.core.order_model import OrderStatus
from gkr_trading.core.operator_controls import KillSwitchLevel
from gkr_trading.core.reconciliation_model import ReconciliationBreak
from gkr_trading.live.base import (
    SubmissionRequest,
    SubmissionResponse,
    VenueAccountInfo,
    VenueAdapter,
    VenuePosition,
)
from gkr_trading.live.reconciliation_service import ReconciliationService
from gkr_trading.live.session_supervisor import SessionState, SessionSupervisor
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry
from gkr_trading.persistence.position_store import PositionStore


class MockAdapter(VenueAdapter):
    def __init__(self, positions=None, cash=100000):
        self._positions = positions or []
        self._cash = cash

    @property
    def venue_name(self) -> str:
        return "test"

    def submit_order(self, request):
        return SubmissionResponse(
            client_order_id=request.client_order_id,
            venue_order_id="V-1", success=True,
        )

    def cancel_order(self, coid):
        return True

    def get_order_status(self, coid):
        return None

    def get_positions(self) -> List[VenuePosition]:
        return self._positions

    def get_account(self) -> VenueAccountInfo:
        return VenueAccountInfo(cash_cents=self._cash, buying_power_cents=self._cash)


@pytest.fixture
def supervisor_components(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    event_store = SqliteEventStore(conn)
    pending = PendingOrderRegistry(conn)
    position_store = PositionStore(conn)
    adapter = MockAdapter()
    recon = ReconciliationService(position_store, adapter, "sess-1", pending_registry=pending)
    sup = SessionSupervisor(event_store, pending, recon, session_id="sess-1", venue="test")
    return sup, event_store, pending, adapter


class TestStartupReconciliation:
    def test_clean_startup_succeeds(self, supervisor_components):
        sup, event_store, pending, adapter = supervisor_components
        assert sup.state == SessionState.INITIALIZING
        result = sup.startup()
        assert result is True
        assert sup.state == SessionState.RUNNING

    def test_startup_marks_unknown_orders(self, supervisor_components):
        sup, event_store, pending, adapter = supervisor_components
        # Register a pending order before startup
        pending.register(
            client_order_id="ord-1", intent_id="int-1", session_id="sess-1",
            instrument_ref_json='{}', action="buy_to_open",
            venue="test", quantity=100,
        )
        pending.update_status("ord-1", OrderStatus.SUBMITTED)

        sup.startup()
        # The order should have been marked UNKNOWN during startup
        status = pending.get_status("ord-1")
        assert status == "unknown"

    def test_blocking_breaks_halt_session(self, supervisor_components):
        """If startup reconciliation finds blocking breaks, session halts."""
        sup, event_store, pending, adapter = supervisor_components
        # Create a local position that won't match venue (which has none)
        sup._recon._store.upsert_equity(
            ticker="AAPL", venue="test", session_id="sess-1",
            signed_qty=100, cost_basis_cents=1500000,
            realized_pnl_cents=0, status="open",
        )
        # Register an order for AAPL so session-scoped recon treats it as blocking.
        pending.register(
            client_order_id="ord-block-1", intent_id="int-block-1",
            session_id="sess-1",
            instrument_ref_json='{"type": "equity", "ticker": "AAPL"}',
            action="buy", venue="test", quantity=100,
        )
        # Adapter returns no positions — this creates a blocking break
        result = sup.startup()
        assert result is False
        assert sup.state == SessionState.HALTED


class TestKillSwitch:
    def test_close_only_blocks_new_orders(self, supervisor_components):
        sup, *_ = supervisor_components
        sup.startup()
        sup.activate_kill_switch(KillSwitchLevel.CLOSE_ONLY)
        assert sup.can_submit_orders()  # closing orders still allowed
        assert not sup.can_submit_new_orders()

    def test_full_halt_blocks_all(self, supervisor_components):
        sup, *_ = supervisor_components
        sup.startup()
        sup.activate_kill_switch(KillSwitchLevel.FULL_HALT)
        assert not sup.can_submit_orders()
        assert not sup.can_submit_new_orders()
        assert sup.state == SessionState.HALTED


class TestSuspendResume:
    def test_suspend_and_resume(self, supervisor_components):
        sup, *_ = supervisor_components
        sup.startup()
        assert sup.state == SessionState.RUNNING
        sup.suspend("websocket_disconnect")
        assert sup.state == SessionState.SUSPENDED
        assert not sup.can_submit_orders()
        result = sup.resume()
        assert result is True
        assert sup.state == SessionState.RUNNING


class TestShutdown:
    def test_shutdown_produces_snapshot(self, supervisor_components):
        sup, event_store, *_ = supervisor_components
        sup.startup()
        snapshot = sup.shutdown()
        assert sup.state == SessionState.STOPPED
        assert snapshot.trigger == "shutdown"
        # Events include session_started and session_stopped
        events = event_store.load_session("sess-1")
        event_types = [e.event_type.value for e in events]
        assert "session_started" in event_types
        assert "session_stopped" in event_types
