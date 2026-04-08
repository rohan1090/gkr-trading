"""Tests for Phase 3 operator CLI commands."""
from __future__ import annotations

import json
import sqlite3
import uuid

import pytest

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.payloads import (
    AssignmentReceivedPayload,
    OperatorCommandPayload,
    ReconciliationCompletedPayload,
    SessionStartedPayload,
)
from gkr_trading.core.events.types import EventType
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry
from gkr_trading.persistence.position_store import PositionStore


def _in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Kill-switch command persistence
# ---------------------------------------------------------------------------

class TestKillSwitchPersistence:
    """Kill-switch commands must persist OperatorCommandEvent before execution."""

    def test_kill_switch_event_persisted(self):
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        # Simulate what the CLI command does
        command_id = str(uuid.uuid4())
        event = CanonicalEvent(
            event_type=EventType.OPERATOR_COMMAND,
            occurred_at_utc=_now_iso(),
            payload=OperatorCommandPayload(
                command_id=command_id,
                command_type="kill_switch",
                parameters=json.dumps({"level": "full_halt", "reason": "test"}),
                operator_id="cli",
            ),
        )
        seq = store.append(session_id, event)
        assert seq >= 0

        events = store.load_session(session_id)
        assert len(events) == 1
        assert events[0].event_type == EventType.OPERATOR_COMMAND
        assert events[0].payload.command_type == "kill_switch"

    def test_kill_switch_level_replay(self):
        """Kill switch level can be derived by replaying operator commands."""
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        # Activate full halt
        e1 = CanonicalEvent(
            event_type=EventType.OPERATOR_COMMAND,
            occurred_at_utc=_now_iso(),
            payload=OperatorCommandPayload(
                command_id=str(uuid.uuid4()),
                command_type="kill_switch",
                parameters=json.dumps({"level": "full_halt"}),
                operator_id="cli",
            ),
        )
        store.append(session_id, e1)

        # Then reset
        e2 = CanonicalEvent(
            event_type=EventType.OPERATOR_COMMAND,
            occurred_at_utc=_now_iso(),
            payload=OperatorCommandPayload(
                command_id=str(uuid.uuid4()),
                command_type="kill_switch",
                parameters=json.dumps({"level": "none"}),
                operator_id="cli",
            ),
        )
        store.append(session_id, e2)

        events = store.load_session(session_id)
        # Replay to find current level
        current_level = "none"
        for ev in events:
            if ev.event_type == EventType.OPERATOR_COMMAND:
                params = json.loads(ev.payload.parameters)
                if ev.payload.command_type == "kill_switch":
                    current_level = params.get("level", current_level)

        assert current_level == "none"


# ---------------------------------------------------------------------------
# Options positions display
# ---------------------------------------------------------------------------

class TestOptionsPositionsDisplay:
    """Options positions are readable from position store."""

    def test_options_positions_from_store(self):
        conn = _in_memory_db()
        store = PositionStore(conn)
        session_id = str(uuid.uuid4())

        store.upsert_options(
            occ_symbol="AAPL  250418C00200000",
            venue="alpaca",
            session_id=session_id,
            instrument_ref_json=json.dumps({"underlying": "AAPL", "right": "call"}),
            long_contracts=2,
            short_contracts=0,
            long_premium_paid_cents=15000,
            short_premium_received_cents=0,
            realized_pnl_cents=0,
            status="open",
        )

        positions = store.get_options_positions(session_id, "alpaca")
        assert len(positions) == 1
        assert positions[0]["occ_symbol"] == "AAPL  250418C00200000"
        assert positions[0]["long_contracts"] == 2
        assert positions[0]["status"] == "open"

    def test_closed_positions_filtered(self):
        conn = _in_memory_db()
        store = PositionStore(conn)
        session_id = str(uuid.uuid4())

        store.upsert_options(
            occ_symbol="AAPL  250418C00200000",
            venue="alpaca",
            session_id=session_id,
            instrument_ref_json="{}",
            long_contracts=0,
            short_contracts=0,
            long_premium_paid_cents=0,
            short_premium_received_cents=0,
            realized_pnl_cents=500,
            status="expired",
        )
        store.upsert_options(
            occ_symbol="SPY   250321P00450000",
            venue="alpaca",
            session_id=session_id,
            instrument_ref_json="{}",
            long_contracts=1,
            short_contracts=0,
            long_premium_paid_cents=8000,
            short_premium_received_cents=0,
            realized_pnl_cents=0,
            status="open",
        )

        all_pos = store.get_options_positions(session_id, "alpaca")
        assert len(all_pos) == 2

        open_pos = [p for p in all_pos if p["status"] not in ("expired", "closed")]
        assert len(open_pos) == 1
        assert open_pos[0]["occ_symbol"] == "SPY   250321P00450000"


# ---------------------------------------------------------------------------
# Session status derivation
# ---------------------------------------------------------------------------

class TestSessionStatusDerivation:
    """Session status is derived from event stream."""

    def test_event_counts_include_options_lifecycle(self):
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        # Add session start
        store.append(session_id, CanonicalEvent(
            event_type=EventType.SESSION_STARTED,
            occurred_at_utc=_now_iso(),
            payload=SessionStartedPayload(session_id=session_id, mode="live"),
        ))

        # Add assignment
        store.append(session_id, CanonicalEvent(
            event_type=EventType.ASSIGNMENT_RECEIVED,
            occurred_at_utc=_now_iso(),
            payload=AssignmentReceivedPayload(
                event_id=str(uuid.uuid4()),
                instrument_occ_symbol="AAPL  250418C00200000",
                instrument_underlying="AAPL",
                venue="alpaca",
                contracts_assigned=1,
                strike_cents=20000,
                right="call",
                resulting_equity_delta=100,
                equity_underlying="AAPL",
                assignment_price_cents=20000,
                effective_date="2025-04-18",
                source="auto",
            ),
        ))

        events = store.load_session(session_id)
        counts: dict[str, int] = {}
        for e in events:
            key = e.event_type.value
            counts[key] = counts.get(key, 0) + 1

        assert counts.get("session_started") == 1
        assert counts.get("assignment_received") == 1

    def test_kill_switch_state_from_replay(self):
        """Current kill switch state should be derivable from event replay."""
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        # Activate close_only
        store.append(session_id, CanonicalEvent(
            event_type=EventType.OPERATOR_COMMAND,
            occurred_at_utc=_now_iso(),
            payload=OperatorCommandPayload(
                command_id=str(uuid.uuid4()),
                command_type="kill_switch",
                parameters=json.dumps({"level": "close_only"}),
                operator_id="cli",
            ),
        ))

        events = store.load_session(session_id)
        current_ks = "none"
        for e in events:
            if e.event_type == EventType.OPERATOR_COMMAND:
                params = json.loads(e.payload.parameters)
                if e.payload.command_type == "kill_switch":
                    current_ks = params.get("level", current_ks)

        assert current_ks == "close_only"


# ---------------------------------------------------------------------------
# Reconcile command persistence
# ---------------------------------------------------------------------------

class TestReconcileCommandPersistence:
    """Reconcile commands must persist OperatorCommandEvent."""

    def test_reconcile_event_persisted(self):
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        command_id = str(uuid.uuid4())
        event = CanonicalEvent(
            event_type=EventType.OPERATOR_COMMAND,
            occurred_at_utc=_now_iso(),
            payload=OperatorCommandPayload(
                command_id=command_id,
                command_type="reconcile",
                parameters=json.dumps({"venue": "alpaca", "mode": "on_demand"}),
                operator_id="cli",
            ),
        )
        store.append(session_id, event)

        events = store.load_session(session_id)
        assert len(events) == 1
        assert events[0].payload.command_type == "reconcile"


# ---------------------------------------------------------------------------
# Alert extraction from events
# ---------------------------------------------------------------------------

class TestAlertExtraction:
    """Alerts should be extractable from the event stream."""

    def test_assignment_generates_alert(self):
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        store.append(session_id, CanonicalEvent(
            event_type=EventType.ASSIGNMENT_RECEIVED,
            occurred_at_utc=_now_iso(),
            payload=AssignmentReceivedPayload(
                event_id=str(uuid.uuid4()),
                instrument_occ_symbol="AAPL  250418C00200000",
                instrument_underlying="AAPL",
                venue="alpaca",
                contracts_assigned=1,
                strike_cents=20000,
                right="call",
                resulting_equity_delta=100,
                equity_underlying="AAPL",
                assignment_price_cents=20000,
                effective_date="2025-04-18",
                source="auto",
            ),
        ))

        events = store.load_session(session_id)
        alerts = []
        for e in events:
            if e.event_type == EventType.ASSIGNMENT_RECEIVED:
                alerts.append({
                    "severity": "critical",
                    "category": "assignment",
                    "message": f"Assignment received: {e.payload.instrument_occ_symbol}",
                })

        assert len(alerts) == 1
        assert alerts[0]["severity"] == "critical"
        assert "AAPL" in alerts[0]["message"]

    def test_blocking_reconciliation_generates_alert(self):
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        store.append(session_id, CanonicalEvent(
            event_type=EventType.RECONCILIATION_COMPLETED,
            occurred_at_utc=_now_iso(),
            payload=ReconciliationCompletedPayload(
                snapshot_id=str(uuid.uuid4()),
                trigger="startup",
                status="breaks_found",
                break_count=2,
                blocking_break_count=1,
            ),
        ))

        events = store.load_session(session_id)
        alerts = []
        for e in events:
            if e.event_type == EventType.RECONCILIATION_COMPLETED:
                blocking = e.payload.blocking_break_count
                if blocking > 0:
                    alerts.append({
                        "severity": "critical",
                        "category": "reconciliation_break",
                    })

        assert len(alerts) == 1
        assert alerts[0]["severity"] == "critical"


# ---------------------------------------------------------------------------
# Operator command write-before-execute invariant
# ---------------------------------------------------------------------------

class TestWriteBeforeExecute:
    """All operator commands that change state must persist BEFORE executing."""

    def test_multiple_commands_maintain_order(self):
        conn = _in_memory_db()
        store = SqliteEventStore(conn)
        session_id = str(uuid.uuid4())

        commands = [
            ("kill_switch", {"level": "close_only"}),
            ("reconcile", {"venue": "alpaca"}),
            ("kill_switch", {"level": "full_halt"}),
        ]

        for cmd_type, params in commands:
            store.append(session_id, CanonicalEvent(
                event_type=EventType.OPERATOR_COMMAND,
                occurred_at_utc=_now_iso(),
                payload=OperatorCommandPayload(
                    command_id=str(uuid.uuid4()),
                    command_type=cmd_type,
                    parameters=json.dumps(params),
                    operator_id="cli",
                ),
            ))

        events = store.load_session(session_id)
        assert len(events) == 3

        # Verify ordering
        assert events[0].payload.command_type == "kill_switch"
        params0 = json.loads(events[0].payload.parameters)
        assert params0["level"] == "close_only"

        assert events[1].payload.command_type == "reconcile"

        assert events[2].payload.command_type == "kill_switch"
        params2 = json.loads(events[2].payload.parameters)
        assert params2["level"] == "full_halt"


# ---------------------------------------------------------------------------
# Equity positions
# ---------------------------------------------------------------------------

class TestEquityPositionsDisplay:
    """Equity positions are readable from position store."""

    def test_equity_positions_from_store(self):
        conn = _in_memory_db()
        store = PositionStore(conn)
        session_id = str(uuid.uuid4())

        store.upsert_equity(
            ticker="AAPL",
            venue="alpaca",
            session_id=session_id,
            signed_qty=100,
            cost_basis_cents=15000000,
            realized_pnl_cents=0,
            status="open",
        )

        positions = store.get_equity_positions(session_id, "alpaca")
        assert len(positions) == 1
        assert positions[0]["ticker"] == "AAPL"
        assert positions[0]["signed_qty"] == 100
