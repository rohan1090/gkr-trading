"""Tests for Alpaca options adapter — single-leg options support."""
from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.live.base import SubmissionRequest
from gkr_trading.live.traditional.alpaca.alpaca_options_adapter import AlpacaOptionsAdapter
from gkr_trading.live.traditional.alpaca.alpaca_options_fill_translator import (
    AlpacaOptionsFillTranslator,
    get_nta_event_type,
    is_nta_lifecycle_event,
)
from gkr_trading.live.traditional.options.options_domain import OCCSymbolParser


def _ref(right="call", strike=20000):
    return OptionsRef(
        underlying="AAPL", expiry=date(2025, 12, 19),
        strike_cents=strike, right=right, style="american",
        multiplier=100, deliverable="AAPL",
        occ_symbol=f"AAPL251219{'C' if right == 'call' else 'P'}{strike * 10:08d}",
    )


class MockHttpClient:
    def __init__(self, responses=None, should_fail=False):
        self.calls = []
        self._responses = responses or {}
        self._should_fail = should_fail

    def post(self, path: str, body: Any = None) -> dict:
        self.calls.append(("POST", path, body))
        if self._should_fail:
            raise ConnectionError("Simulated failure")
        return self._responses.get(("POST", path), {"id": "alpaca-order-123"})

    def get(self, path: str) -> Any:
        self.calls.append(("GET", path, None))
        return self._responses.get(("GET", path), [])


class TestAlpacaOptionsSubmission:
    def test_submit_options_order_with_limit(self):
        http = MockHttpClient()
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        request = SubmissionRequest(
            client_order_id="ord-1",
            instrument_ref=_ref(),
            action="buy_to_open",
            quantity=5,
            limit_price_cents=350,
            time_in_force="day",
            venue="alpaca_paper",
        )
        response = adapter.submit_options_order(request)
        assert response.success
        assert response.venue_order_id == "alpaca-order-123"
        # Verify position_intent was sent
        _, _, body = http.calls[0]
        assert body["position_intent"] == "buy_to_open"
        assert body["type"] == "limit"
        assert body["limit_price"] == "3.5"

    def test_submit_options_order_without_limit_rejected(self):
        http = MockHttpClient()
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        request = SubmissionRequest(
            client_order_id="ord-1",
            instrument_ref=_ref(),
            action="buy_to_open",
            quantity=5,
            limit_price_cents=None,  # No limit!
            time_in_force="day",
            venue="alpaca_paper",
        )
        response = adapter.submit_options_order(request)
        assert not response.success
        assert response.rejected
        assert "limit price" in response.reject_reason.lower()

    def test_submit_equity_ref_rejected(self):
        http = MockHttpClient()
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        request = SubmissionRequest(
            client_order_id="ord-1",
            instrument_ref=EquityRef(ticker="AAPL"),
            action="buy_to_open",
            quantity=100,
            limit_price_cents=15000,
            time_in_force="day",
            venue="alpaca_paper",
        )
        response = adapter.submit_options_order(request)
        assert not response.success
        assert "OptionsRef" in response.reject_reason

    def test_submit_failure_returns_error(self):
        http = MockHttpClient(should_fail=True)
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        request = SubmissionRequest(
            client_order_id="ord-1",
            instrument_ref=_ref(),
            action="buy_to_open",
            quantity=1,
            limit_price_cents=500,
            time_in_force="day",
            venue="alpaca_paper",
        )
        response = adapter.submit_options_order(request)
        assert not response.success


class TestAlpacaNTATranslation:
    def test_assignment_nta_detected(self):
        assert is_nta_lifecycle_event({"activity_type": "OASGN"})
        assert get_nta_event_type({"activity_type": "OASGN"}) == "assignment"

    def test_exercise_nta_detected(self):
        assert is_nta_lifecycle_event({"activity_type": "OEXC"})
        assert get_nta_event_type({"activity_type": "OEXC"}) == "exercise"

    def test_expiration_nta_detected(self):
        assert is_nta_lifecycle_event({"activity_type": "OEXP"})
        assert get_nta_event_type({"activity_type": "OEXP"}) == "expiration"

    def test_fill_is_not_nta(self):
        assert not is_nta_lifecycle_event({"activity_type": "FILL"})

    def test_translate_assignment_event(self):
        http = MockHttpClient()
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        event = adapter.translate_assignment({
            "symbol": "AAPL251219P00200000",
            "qty": "2",
            "date": "2025-12-19",
        })
        assert event is not None
        assert event.contracts_assigned == 2
        assert event.right == "put"
        assert event.resulting_equity_delta == 200  # put: buy shares

    def test_translate_exercise_event(self):
        http = MockHttpClient()
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        event = adapter.translate_exercise({
            "symbol": "AAPL251219C00200000",
            "qty": "1",
            "date": "2025-12-19",
        })
        assert event is not None
        assert event.contracts_exercised == 1
        assert event.right == "call"
        assert event.resulting_equity_delta == 100  # call: receive shares

    def test_translate_expiration_event(self):
        http = MockHttpClient()
        adapter = AlpacaOptionsAdapter(http, session_id="sess-1")
        event = adapter.translate_expiration({
            "symbol": "AAPL251219C00200000",
            "qty": "3",
        })
        assert event is not None
        assert event.contracts_expired == 3


class TestOCCSymbolParser:
    def test_parse_call(self):
        ref = OCCSymbolParser.parse("AAPL251219C00200000")
        assert ref.underlying == "AAPL"
        assert ref.expiry == date(2025, 12, 19)
        assert ref.right == "call"
        assert ref.strike_cents == 20000  # $200
        assert ref.multiplier == 100

    def test_parse_put(self):
        ref = OCCSymbolParser.parse("TSLA260116P00025000")
        assert ref.underlying == "TSLA"
        assert ref.expiry == date(2026, 1, 16)
        assert ref.right == "put"
        assert ref.strike_cents == 2500  # $25

    def test_generate(self):
        symbol = OCCSymbolParser.generate("AAPL", date(2025, 12, 19), "call", 20000)
        assert symbol == "AAPL251219C00200000"

    def test_roundtrip(self):
        original = "AAPL251219C00200000"
        ref = OCCSymbolParser.parse(original)
        regenerated = OCCSymbolParser.generate(
            ref.underlying, ref.expiry, ref.right, ref.strike_cents
        )
        assert regenerated == original


class TestAlpacaOptionsFillTranslator:
    def test_options_fill_translated(self):
        translator = AlpacaOptionsFillTranslator(session_id="sess-1")
        fill = translator.translate_fill({
            "activity_type": "FILL",
            "symbol": "AAPL251219C00200000",
            "asset_class": "us_option",
            "qty": "5",
            "price": "3.50",
            "side": "buy",
            "position_intent": "buy_to_open",
            "order_id": "ord-1",
        })
        assert fill is not None
        assert isinstance(fill.instrument_ref, OptionsRef)
        assert fill.quantity == 5
        assert fill.price_cents == 350
        assert fill.action == "buy_to_open"

    def test_equity_fill_ignored(self):
        translator = AlpacaOptionsFillTranslator(session_id="sess-1")
        fill = translator.translate_fill({
            "activity_type": "FILL",
            "symbol": "AAPL",
            "asset_class": "us_equity",
            "qty": "100",
            "price": "150.00",
            "side": "buy",
        })
        assert fill is None
