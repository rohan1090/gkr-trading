from __future__ import annotations

from decimal import Decimal

import pytest

from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId
from gkr_trading.live.alpaca_paper_mapping import (
    AlpacaMalformedPayloadError,
    fill_activity_to_broker_fill_fact,
    order_to_lifecycle_facts,
)
from gkr_trading.live.broker_adapter import SubmitRequest

IID = InstrumentId("00000000-0000-4000-8000-00000000aa01")
OID = OrderId("00000000-0000-4000-8000-00000000aa02")


def _req() -> SubmitRequest:
    return SubmitRequest(
        order_id=OID,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("10"),
        order_type=OrderType.MARKET,
        limit_price=None,
        executable_broker_symbol="DEMO",
        context_ts_utc="2024-01-01T12:00:00Z",
    )


def test_fill_activity_maps_execution_id_and_qty() -> None:
    act = {
        "id": "act-777",
        "activity_type": "FILL",
        "order_id": "alp-1",
        "qty": "10",
        "price": "100.5",
        "side": "buy",
        "transaction_time": "2024-06-01T15:30:00Z",
    }
    f = fill_activity_to_broker_fill_fact(act, submit=_req(), fallback_ts="2024-01-01T00:00:00Z")
    assert f.broker_execution_id == "act-777"
    assert f.quantity == Decimal("10")
    assert f.price == Decimal("100.5")
    assert f.client_order_id == OID


def test_fill_activity_missing_id_raises() -> None:
    with pytest.raises(AlpacaMalformedPayloadError):
        fill_activity_to_broker_fill_fact(
            {"order_id": "x", "qty": "1", "price": "1"},
            submit=_req(),
            fallback_ts="2024-01-01T00:00:00Z",
        )


def test_order_rejected_fact() -> None:
    o = {
        "status": "rejected",
        "updated_at": "2024-01-02T10:00:00Z",
        "reject_reason": "insufficient qty",
    }
    facts = order_to_lifecycle_facts(o, submit=_req(), fallback_ts="2024-01-01T00:00:00Z")
    assert len(facts) == 1
    assert facts[0].kind == "reject"
    assert facts[0].reason_detail == "insufficient qty"


def test_order_cancelled_fact() -> None:
    o = {"status": "canceled", "updated_at": "2024-01-02T10:00:00Z"}
    facts = order_to_lifecycle_facts(o, submit=_req(), fallback_ts="2024-01-01T00:00:00Z")
    assert len(facts) == 1
    assert facts[0].kind == "cancel"


def test_filled_order_no_lifecycle() -> None:
    o = {"status": "filled"}
    assert order_to_lifecycle_facts(o, submit=_req(), fallback_ts="2024-01-01T00:00:00Z") == []
