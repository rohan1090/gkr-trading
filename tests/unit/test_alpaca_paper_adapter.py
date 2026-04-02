from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId
from gkr_trading.live.alpaca_config import AlpacaPaperConfig
from gkr_trading.live.alpaca_http import AlpacaHttpError, AlpacaSubmitUncertaintyError
from gkr_trading.live.alpaca_paper_adapter import AlpacaPaperAdapter, _build_order_body
from gkr_trading.live.broker_adapter import BrokerPollHints, BrokerSyncPhase, SubmitRequest

CFG = AlpacaPaperConfig(api_key="k", secret_key="s", base_url="https://paper-api.alpaca.markets")
IID = InstrumentId("00000000-0000-4000-8000-00000000bb01")
OID = OrderId("00000000-0000-4000-8000-00000000bb02")


class FakeAlpacaHttp:
    def __init__(self, queue: list[Any]) -> None:
        self._q = list(queue)
        self.calls: list[tuple[str, str, dict[str, str] | None, dict[str, Any] | None]] = []

    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((method, path, query, json_body))
        if not self._q:
            raise RuntimeError("FakeAlpacaHttp: empty response queue")
        item = self._q.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _req(**kwargs: Any) -> SubmitRequest:
    base = dict(
        order_id=OID,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("5"),
        order_type=OrderType.MARKET,
        limit_price=None,
        executable_broker_symbol="AAPL",
        context_ts_utc="2024-01-05T16:00:00Z",
    )
    base.update(kwargs)
    return SubmitRequest(**base)


def test_build_order_body_market() -> None:
    b = _build_order_body(_req())
    assert b["symbol"] == "AAPL"
    assert b["client_order_id"] == str(OID)
    assert b["qty"] == "5"
    assert b["type"] == "market"


def test_build_order_body_requires_symbol() -> None:
    with pytest.raises(ValueError, match="executable_broker_symbol"):
        _build_order_body(_req(executable_broker_symbol=None))


def test_build_order_body_rejects_fractional_shares() -> None:
    with pytest.raises(ValueError, match="whole-share"):
        _build_order_body(_req(quantity=Decimal("1.5")))


def test_submit_success_registers_tracking() -> None:
    http = FakeAlpacaHttp(
        [{"id": "alp-oid-1", "status": "accepted", "client_order_id": str(OID)}]
    )
    ad = AlpacaPaperAdapter(CFG, http=http)
    r = ad.submit(_req())
    assert not r.rejected
    assert r.broker_order_id == "alp-oid-1"
    assert str(OID) in ad._by_client


def test_submit_http_422_rejected_no_track() -> None:
    err = AlpacaHttpError(422, '{"message":"qty must be positive"}', {"message": "qty must be positive"})
    http = FakeAlpacaHttp([err])
    ad = AlpacaPaperAdapter(CFG, http=http)
    r = ad.submit(_req())
    assert r.rejected
    assert str(OID) not in ad._by_client


def test_submit_503_raises_uncertainty() -> None:
    err = AlpacaHttpError(503, "unavailable", None)
    http = FakeAlpacaHttp([err])
    ad = AlpacaPaperAdapter(CFG, http=http)
    with pytest.raises(AlpacaSubmitUncertaintyError) as ei:
        ad.submit(_req())
    assert ei.value.client_order_id == str(OID)


def test_submit_timeout_raises_uncertainty() -> None:
    http = FakeAlpacaHttp([AlpacaSubmitUncertaintyError("timeout", client_order_id=None)])
    ad = AlpacaPaperAdapter(CFG, http=http)
    with pytest.raises(AlpacaSubmitUncertaintyError):
        ad.submit(_req())


def test_poll_emits_fill_and_skips_duplicate_activity_id() -> None:
    fill = {
        "id": "fill-act-1",
        "activity_type": "FILL",
        "order_id": "alp-oid-1",
        "qty": "5",
        "price": "190.1",
        "side": "buy",
        "transaction_time": "2024-01-05T16:01:00Z",
    }
    http = FakeAlpacaHttp(
        [
            {"id": "alp-oid-1", "status": "accepted", "client_order_id": str(OID)},
            [],  # orders list poll 1
            [fill],  # activities poll 1
            [],  # orders poll 2
            [fill],  # activities poll 2 — same id
        ]
    )
    ad = AlpacaPaperAdapter(CFG, http=http)
    ad.submit(_req())
    h = BrokerPollHints(bar_ts_utc="2024-01-05T16:01:00Z", default_occurred_at_utc="2024-01-05T16:01:00Z")
    r1 = ad.poll_broker_facts(cursor=None, hints=h, phase=BrokerSyncPhase.POST_SUBMIT)
    r2 = ad.poll_broker_facts(cursor=r1.cursor, hints=h, phase=BrokerSyncPhase.POST_BAR)
    n_fill = sum(1 for f in r1.facts + r2.facts if f.kind == "fill")
    assert n_fill == 1


def test_poll_cancel_for_tracked_order() -> None:
    http = FakeAlpacaHttp(
        [
            {"id": "alp-oid-1", "status": "accepted", "client_order_id": str(OID)},
            [
                {
                    "id": "alp-oid-1",
                    "client_order_id": str(OID),
                    "status": "canceled",
                    "updated_at": "2024-01-05T17:00:00Z",
                }
            ],
            [],
        ]
    )
    ad = AlpacaPaperAdapter(CFG, http=http)
    ad.submit(_req())
    h = BrokerPollHints(default_occurred_at_utc="2024-01-05T17:00:00Z")
    r = ad.poll_broker_facts(cursor=None, hints=h, phase=BrokerSyncPhase.POST_BAR)
    assert any(f.kind == "cancel" for f in r.facts)


def test_config_from_env_missing_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from gkr_trading.live.alpaca_config import AlpacaConfigError, AlpacaPaperConfig

    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    with pytest.raises(AlpacaConfigError):
        AlpacaPaperConfig.from_env()
