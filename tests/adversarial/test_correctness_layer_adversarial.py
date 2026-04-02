"""
Adversarial pass against correctness hardening: fill ids, lifecycle, replay strictness.

These tests are designed to falsify claims — a failing test means a real hole.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from gkr_trading.core.events.builders import fill_received, order_acknowledged, order_submitted
from gkr_trading.core.events.payloads import FillReceivedPayload
from gkr_trading.core.portfolio import StrictReplayError
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import FillId, InstrumentId, OrderId

from tests.adversarial._streams import IID, base_submit_fill


def test_ack_before_submit_is_silent_noop_then_normal_flow() -> None:
    """Lifecycle: orphan ack does not corrupt; later submit+fill still works."""
    oid = OrderId("00000000-0000-4000-8000-00000000abad1")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_acknowledged(oid, ts, broker_order_id="early"),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("100000")).state
    assert s.positions[str(IID)] == Decimal("10")
    assert str(oid) not in s.open_orders


def test_duplicate_fill_replay_twice_idempotent_on_applied_fill_set() -> None:
    """Same session replayed: applied_fill_ids is reconstructed; duplicate row still idempotent."""
    oid = OrderId("00000000-0000-4000-8000-00000000abad2")
    ts = "2024-01-01T12:00:00Z"
    ev = base_submit_fill(oid, ts=ts) + [
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    a = replay_portfolio_state(ev, Decimal("100000")).state
    b = replay_portfolio_state(ev, Decimal("100000")).state
    assert a == b
    assert len(a.fill_history) == 1
    assert len(a.applied_fill_ids) == 1


def test_two_partial_fills_same_deterministic_hash_without_salt_collide() -> None:
    """
    Falsification: identical (order, ts, qty, price, fees) → identical fill_id → second fill dropped.

    Operational implication: partial fills at same tick/price must use distinct fill_id,
    broker_execution_id, or dedupe_salt.
    """
    oid = OrderId("00000000-0000-4000-8000-00000000abad3")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("5"), Decimal("100"), ts, ts
        ),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("5"), Decimal("100"), ts, ts
        ),
    ]
    s = replay_portfolio_state(ev, Decimal("100000")).state
    assert s.positions.get(str(IID)) == Decimal("5")
    assert len(s.fill_history) == 1
    assert s.open_orders[str(oid)].remaining_qty == Decimal("5")


def test_broker_execution_id_must_match_fill_id_when_both_present() -> None:
    """Canonical policy: exec:{broker_execution_id} must equal fill_id or validation fails."""
    with pytest.raises(ValidationError):
        FillReceivedPayload.model_validate(
            {
                "order_id": str(OrderId("00000000-0000-4000-8000-00000000abad4")),
                "instrument_id": str(IID),
                "side": "buy",
                "fill_qty": "1",
                "fill_price": "10",
                "fill_ts_utc": "2024-01-01T00:00:00Z",
                "fill_id": "v1:custom_explicit_id",
                "broker_execution_id": "SHOULD_NOT_OVERRIDE",
            }
        )


def test_strict_replay_rejects_second_fill_after_order_filled() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000abad5")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts, dedupe_salt="a"
        ),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("1"), Decimal("100"), ts, ts, dedupe_salt="b"
        ),
    ]
    with pytest.raises(StrictReplayError) as ei:
        replay_portfolio_state(ev, Decimal("100000"), strict=True)
    assert ei.value.code == "FILL_AFTER_FILLED"


def test_fill_before_submit_permissive_tolerates_out_of_order_log() -> None:
    """Malformed log order: economics still composes; not a correctness guarantee for production."""
    oid = OrderId("00000000-0000-4000-8000-00000000abad6")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        fill_received(oid, IID, OrderSide.BUY, Decimal("3"), Decimal("50"), ts, ts, dedupe_salt="orph"),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(
            oid, IID, OrderSide.BUY, Decimal("7"), Decimal("50"), ts, ts, dedupe_salt="rest"
        ),
    ]
    s = replay_portfolio_state(ev, Decimal("100000")).state
    assert s.positions[str(IID)] == Decimal("10")


def test_orphan_fill_surfaces_anomaly_in_replay_result_by_default() -> None:
    """ReplayResult always carries anomaly tuples — operators see ORPHAN_FILL without extra flags."""
    oid = OrderId("00000000-0000-4000-8000-00000000abad7")
    ts = "2024-01-01T12:00:00Z"
    ev = [fill_received(oid, IID, OrderSide.BUY, Decimal("1"), Decimal("1"), ts, ts)]
    r = replay_portfolio_state(ev, Decimal("100000"))
    assert len(r.anomalies) == 1
    assert r.anomalies[0].code == "ORPHAN_FILL"


def test_malformed_fill_payload_still_fails_at_load() -> None:
    """Typed field errors surface as ValidationError once fill_id is explicit (skips legacy hash path)."""
    with pytest.raises(ValidationError):
        FillReceivedPayload.model_validate(
            {
                "order_id": str(OrderId("00000000-0000-4000-8000-00000000abad8")),
                "instrument_id": str(IID),
                "side": "buy",
                "fill_qty": "1",
                "fill_price": "not_a_decimal",
                "fill_ts_utc": "2024-01-01T00:00:00Z",
                "fill_id": "v1:explicit_so_skip_legacy_normalizer",
            }
        )


def test_duplicate_submit_while_open_still_idempotent() -> None:
    oid = OrderId("00000000-0000-4000-8000-00000000abad9")
    ts = "2024-01-01T12:00:00Z"
    ev = [
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("10"), OrderType.MARKET, None, ts),
        fill_received(oid, IID, OrderSide.BUY, Decimal("10"), Decimal("100"), ts, ts),
    ]
    s = replay_portfolio_state(ev, Decimal("100000")).state
    assert s.positions[str(IID)] == Decimal("10")


