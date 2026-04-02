"""Deterministic broker fact sorting for within-sync append batches."""

from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.schemas.enums import OrderSide
from gkr_trading.core.schemas.ids import InstrumentId, OrderId
from gkr_trading.live.broker_adapter import (
    BrokerFillFact,
    BrokerOrderCancelledFact,
    BrokerOrderRejectedFact,
)
from gkr_trading.core.events.builders import fill_received, order_submitted
from gkr_trading.core.events.types import EventType
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.sessions.manager import SessionManager
from gkr_trading.live.broker_sync import (
    append_sorted_broker_facts,
    seen_broker_execution_ids_from_events,
    sort_broker_facts_for_append,
)
from gkr_trading.persistence.event_store import SqliteEventStore

IID = InstrumentId("00000000-0000-4000-8000-000000000099")
OID_A = OrderId("00000000-0000-4000-8000-00000000aa01")
OID_B = OrderId("00000000-0000-4000-8000-00000000aa02")


def test_sort_reject_before_cancel_before_fill() -> None:
    f_fill = BrokerFillFact(
        client_order_id=OID_A,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        price=Decimal("10"),
        fees=Decimal("0"),
        fill_ts_utc="2024-01-01T12:00:00Z",
        occurred_at_utc="2024-01-01T09:00:00Z",
        broker_execution_id="ex-z",
    )
    f_rej = BrokerOrderRejectedFact(
        client_order_id=OID_B,
        reason_code="X",
        occurred_at_utc="2024-01-01T08:00:00Z",
    )
    f_can = BrokerOrderCancelledFact(
        client_order_id=OID_A,
        occurred_at_utc="2024-01-01T07:00:00Z",
    )
    out = sort_broker_facts_for_append((f_fill, f_rej, f_can))
    assert [type(x).__name__ for x in out] == [
        "BrokerOrderRejectedFact",
        "BrokerOrderCancelledFact",
        "BrokerFillFact",
    ]


def test_sort_same_class_by_occurred_at_then_tie_breaker() -> None:
    f2 = BrokerFillFact(
        client_order_id=OID_B,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        price=Decimal("1"),
        fees=Decimal("0"),
        fill_ts_utc="2024-01-01T00:00:00Z",
        occurred_at_utc="2024-01-01T00:00:00Z",
        broker_execution_id="ex-b",
    )
    f1 = BrokerFillFact(
        client_order_id=OID_A,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        price=Decimal("1"),
        fees=Decimal("0"),
        fill_ts_utc="2024-01-01T00:00:00Z",
        occurred_at_utc="2024-01-01T00:00:00Z",
        broker_execution_id="ex-a",
    )
    out = sort_broker_facts_for_append((f2, f1))
    assert out[0].broker_execution_id == "ex-a"
    assert out[1].broker_execution_id == "ex-b"


def test_seen_broker_execution_ids_from_events_extracts_payload() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000bb10")
    ts = "2024-01-01T00:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("1"), OrderType.MARKET, None, ts),
        fill_received(
            oid,
            IID,
            OrderSide.BUY,
            Decimal("1"),
            Decimal("10"),
            ts,
            ts,
            broker_execution_id="be-1",
        ),
    ]
    s = seen_broker_execution_ids_from_events(ev)
    assert s == {"be-1"}


def test_append_sorted_skips_seen_broker_execution_id(tmp_path) -> None:
    from gkr_trading.cli import seed
    from gkr_trading.core.schemas.ids import SessionId

    db = str(tmp_path / "e.db")
    conn = seed.initialize_database(db)
    store = SqliteEventStore(conn)
    sm = SessionManager(store, SessionId("00000000-0000-4000-8000-00000000aa99"))
    state = __import__(
        "gkr_trading.core.portfolio", fromlist=["PortfolioState"]
    ).PortfolioState.initial(Decimal("100000"))
    seen: set[str] = {"dup-exec"}
    facts = (
        BrokerFillFact(
            client_order_id=OID_A,
            instrument_id=IID,
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            price=Decimal("1"),
            fees=Decimal("0"),
            fill_ts_utc="2024-01-01T00:00:00Z",
            occurred_at_utc="2024-01-01T00:00:00Z",
            broker_execution_id="dup-exec",
        ),
    )
    state2 = append_sorted_broker_facts(sm, state, facts, seen_broker_execution_ids=seen)
    loaded = store.load_session("00000000-0000-4000-8000-00000000aa99")
    conn.close()
    fills = [e for e in loaded if e.event_type == EventType.FILL_RECEIVED]
    assert len(fills) == 0
    assert state2 is state
