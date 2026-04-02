"""Scenarios 1–10, 3–7, 9–10: event store semantics and replay."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from gkr_trading.core.events.builders import market_bar, order_submitted, portfolio_updated
from gkr_trading.core.events.builders import fill_received as fill_ev
from gkr_trading.core.events.envelope import SCHEMA_VERSION, EventEnvelope
from gkr_trading.core.events.serde import dumps_event, loads_event
from gkr_trading.core.events.types import EventType
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId, SessionId
from gkr_trading.core.events.builders import order_acknowledged
from gkr_trading.persistence.db import open_sqlite
from gkr_trading.persistence.event_store import SqliteEventStore

from tests.adversarial._streams import IID, base_submit_fill, session_dupes


def test_duplicate_fill_same_fill_id_is_idempotent() -> None:
    """1 — same canonical fill_id must not double-apply."""
    oid = OrderId("00000000-0000-4000-8000-00000000b001")
    ts = "2024-01-01T12:00:00Z"
    ev = base_submit_fill(oid, ts=ts)
    dup = fill_ev(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts)
    s = replay_portfolio_state(ev + [dup], Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("10")
    assert len(s.fill_history) == 1


def test_two_distinct_fills_same_order_increase_position() -> None:
    """Two broker executions → two fill_ids → two applications."""
    oid = OrderId("00000000-0000-4000-8000-00000000b001b")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("20"), OrderType.MARKET, None, ts),
        fill_ev(
            oid,
            IID,
            OrderSide.BUY,
            Decimal("10"),
            Decimal("100"),
            ts,
            ts,
            dedupe_salt="leg-a",
        ),
        fill_ev(
            oid,
            IID,
            OrderSide.BUY,
            Decimal("10"),
            Decimal("100"),
            ts,
            ts,
            dedupe_salt="leg-b",
        ),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("20")
    assert len(s.fill_history) == 2


def test_duplicate_order_acknowledged_no_position_change() -> None:
    """2 — ack is no-op for portfolio."""
    oid = OrderId("00000000-0000-4000-8000-00000000b002")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_acknowledged(oid, ts),
        order_acknowledged(oid, ts),
        fill_ev(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("10")
    assert s.cash == Decimal("9000")


def test_duplicate_session_events_replay_unchanged() -> None:
    """3 — session_* skipped by replay; economics match trade-only stream."""
    sid = SessionId("00000000-0000-4000-8000-00000000b003")
    ts = "2024-01-01T12:00:00Z"
    oid = OrderId("00000000-0000-4000-8000-00000000b004")
    ev = session_dupes(sid, ts) + base_submit_fill(oid, ts=ts)
    core = base_submit_fill(oid, ts=ts)
    assert replay_portfolio_state(ev, Decimal("10000")).state == replay_portfolio_state(core, Decimal("10000")).state


def test_replay_same_events_twice_identical() -> None:
    """4 — pure replay idempotence on inputs."""
    oid = OrderId("00000000-0000-4000-8000-00000000b005")
    ev = base_submit_fill(oid)
    x = replay_portfolio_state(ev, Decimal("10000")).state
    y = replay_portfolio_state(ev, Decimal("10000")).state
    assert x.cash == y.cash and x.positions == y.positions and x.realized_pnl == y.realized_pnl


def test_strip_portfolio_updated_replay_unchanged() -> None:
    """5 — replay does not depend on portfolio_updated."""
    oid = OrderId("00000000-0000-4000-8000-00000000b006")
    ts = "2024-01-01T12:00:00Z"
    core = base_submit_fill(oid, ts=ts)
    st = replay_portfolio_state(core, Decimal("10000")).state
    pu = portfolio_updated(st, ts)
    with_audit = core + [pu, pu]
    assert replay_portfolio_state(with_audit, Decimal("10000")).state == st


def test_inject_audit_only_events_unchanged() -> None:
    """6 — extra session + signal path skipped in replay fold."""
    from gkr_trading.core.events.builders import session_started, signal_generated
    from gkr_trading.core.events.builders import trade_intent_created
    from gkr_trading.core.intents.models import TradeIntent
    from gkr_trading.core.schemas.ids import IntentId, new_intent_id

    oid = OrderId("00000000-0000-4000-8000-00000000b007")
    ts = "2024-01-01T12:00:00Z"
    intent = TradeIntent(
        intent_id=new_intent_id(),
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("10"),
        order_type=OrderType.MARKET,
        strategy_name="x",
    )
    noise = [
        session_started(SessionId("00000000-0000-4000-8000-00000000c099"), "paper", ts),
        signal_generated("x", IID, "n", ts),
        trade_intent_created(intent, ts),
    ]
    core = base_submit_fill(oid, ts=ts)
    assert replay_portfolio_state(noise + core, Decimal("10000")).state == replay_portfolio_state(
        core, Decimal("10000")
    ).state


def test_reorder_ack_before_fill_same_as_fill_after_ack() -> None:
    """7 — ack ordering irrelevant."""
    oid = OrderId("00000000-0000-4000-8000-00000000b008")
    ts = "2024-01-01T12:00:00Z"
    a = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_acknowledged(oid, ts),
        fill_ev(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    b = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_ev(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
        order_acknowledged(oid, ts),
    ]
    assert replay_portfolio_state(a, Decimal("10000")).state == replay_portfolio_state(b, Decimal("10000")).state


def test_fill_without_prior_submit_still_mutates_position() -> None:
    """8 — V1: orphan fills apply (honest degradation = no silent drop)."""
    oid = OrderId("00000000-0000-4000-8000-00000000b009")
    ts = "2024-01-01T12:00:00Z"
    ev = [fill_ev(oid, IID, OrderSide.BUY, Decimal("5"), Decimal("50"), ts, ts)]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.positions[str(IID)] == Decimal("5")
    assert str(oid) not in s.open_orders


def test_unknown_event_type_on_load_fails() -> None:
    """9 — invalid enum rejected."""
    raw = '{"schema_version":1,"event_type":"not_a_real_type","occurred_at_utc":"2024-01-01T00:00:00Z","payload":{}}'
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate_json(raw)


def test_schema_version_mismatch_fails() -> None:
    env = EventEnvelope(
        schema_version=999,
        event_type=EventType.SESSION_STARTED,
        occurred_at_utc="2024-01-01T00:00:00Z",
        payload={"session_id": str(SessionId("00000000-0000-4000-8000-00000000d001")), "mode": "x"},
    )
    raw = env.model_dump_json()
    with pytest.raises(ValueError, match="Unsupported schema_version"):
        loads_event(raw)


def test_corrupted_payload_validation_error() -> None:
    """10 — honest failure."""
    env = EventEnvelope(
        schema_version=SCHEMA_VERSION,
        event_type=EventType.FILL_RECEIVED,
        occurred_at_utc="2024-01-01T00:00:00Z",
        payload={"order_id": "not-uuid"},
    )
    raw = env.model_dump_json()
    with pytest.raises(ValidationError):
        loads_event(raw)


def test_sqlite_load_session_propagates_bad_row(tmp_path) -> None:
    db = str(tmp_path / "bad.db")
    conn = open_sqlite(db)
    conn.executescript(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            envelope_json TEXT NOT NULL,
            UNIQUE(session_id, seq)
        );
        INSERT INTO events(session_id, seq, envelope_json) VALUES ('s', 1, '{"bad":true}');
        """
    )
    store = SqliteEventStore(conn)
    with pytest.raises(Exception):
        store.load_session("s")
    conn.close()


def test_duplicate_session_started_replay_bitwise() -> None:
    """3b — duplicate session lines + one trade."""
    sid = SessionId("00000000-0000-4000-8000-00000000e001")
    ts = "2024-01-01T12:00:00Z"
    oid = OrderId("00000000-0000-4000-8000-00000000e002")
    ev = session_dupes(sid, ts) + base_submit_fill(oid, ts=ts)
    s1 = replay_portfolio_state(ev, Decimal("10000")).state
    s2 = replay_portfolio_state(ev, Decimal("10000")).state
    assert s1.cash == s2.cash and s1.positions == s2.positions


def test_duplicate_order_submitted_idempotent_preserves_partial() -> None:
    """ORDER_SUBMITTED duplicate does not reset remaining after partial fill."""
    oid = OrderId("00000000-0000-4000-8000-00000000f001")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_ev(oid, IID, OrderSide.BUY, Decimal("4"), Decimal("100"), ts, ts),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("10000")).state
    assert s.open_orders[str(oid)].remaining_qty == Decimal("6")
