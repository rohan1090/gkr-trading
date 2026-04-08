"""Tests for InstrumentRef, EquityRef, OptionsRef."""
from __future__ import annotations

from datetime import date

import pytest

from gkr_trading.core.instruments import EquityRef, InstrumentRef, OptionsRef


class TestEquityRef:
    def test_create(self):
        ref = EquityRef(ticker="AAPL")
        assert ref.asset_class == "equity"
        assert ref.ticker == "AAPL"
        assert ref.canonical_key == "equity:AAPL"

    def test_frozen(self):
        ref = EquityRef(ticker="AAPL")
        with pytest.raises(AttributeError):
            ref.ticker = "GOOG"  # type: ignore[misc]

    def test_empty_ticker_rejected(self):
        with pytest.raises(ValueError, match="ticker must be non-empty"):
            EquityRef(ticker="")

    def test_blank_ticker_rejected(self):
        with pytest.raises(ValueError, match="ticker must be non-empty"):
            EquityRef(ticker="   ")

    def test_equality(self):
        a = EquityRef(ticker="AAPL")
        b = EquityRef(ticker="AAPL")
        assert a == b

    def test_inequality(self):
        a = EquityRef(ticker="AAPL")
        b = EquityRef(ticker="GOOG")
        assert a != b

    def test_hashable(self):
        ref = EquityRef(ticker="AAPL")
        s = {ref}
        assert ref in s


class TestOptionsRef:
    @pytest.fixture
    def aapl_call(self) -> OptionsRef:
        return OptionsRef(
            underlying="AAPL",
            expiry=date(2025, 12, 19),
            strike_cents=20000,
            right="call",
            style="american",
            multiplier=100,
            deliverable="AAPL",
            occ_symbol="AAPL251219C00200000",
        )

    def test_create(self, aapl_call: OptionsRef):
        assert aapl_call.asset_class == "option"
        assert aapl_call.underlying == "AAPL"
        assert aapl_call.strike_cents == 20000
        assert aapl_call.strike_dollars == 200.0
        assert aapl_call.right == "call"
        assert aapl_call.canonical_key == "option:AAPL251219C00200000"

    def test_frozen(self, aapl_call: OptionsRef):
        with pytest.raises(AttributeError):
            aapl_call.strike_cents = 25000  # type: ignore[misc]

    def test_negative_strike_rejected(self):
        with pytest.raises(ValueError, match="strike_cents must be positive"):
            OptionsRef(
                underlying="AAPL", expiry=date(2025, 12, 19),
                strike_cents=-100, right="call", style="american",
                multiplier=100, deliverable="AAPL", occ_symbol="X",
            )

    def test_zero_multiplier_rejected(self):
        with pytest.raises(ValueError, match="multiplier must be positive"):
            OptionsRef(
                underlying="AAPL", expiry=date(2025, 12, 19),
                strike_cents=20000, right="call", style="american",
                multiplier=0, deliverable="AAPL", occ_symbol="X",
            )

    def test_empty_underlying_rejected(self):
        with pytest.raises(ValueError, match="underlying must be non-empty"):
            OptionsRef(
                underlying="", expiry=date(2025, 12, 19),
                strike_cents=20000, right="call", style="american",
                multiplier=100, deliverable="AAPL", occ_symbol="X",
            )

    def test_empty_occ_rejected(self):
        with pytest.raises(ValueError, match="occ_symbol must be non-empty"):
            OptionsRef(
                underlying="AAPL", expiry=date(2025, 12, 19),
                strike_cents=20000, right="call", style="american",
                multiplier=100, deliverable="AAPL", occ_symbol="",
            )

    def test_equality(self, aapl_call: OptionsRef):
        other = OptionsRef(
            underlying="AAPL", expiry=date(2025, 12, 19),
            strike_cents=20000, right="call", style="american",
            multiplier=100, deliverable="AAPL", occ_symbol="AAPL251219C00200000",
        )
        assert aapl_call == other

    def test_hashable(self, aapl_call: OptionsRef):
        s = {aapl_call}
        assert aapl_call in s


class TestInstrumentRefPolymorphism:
    def test_equity_is_instrument(self):
        ref = EquityRef(ticker="AAPL")
        assert isinstance(ref, InstrumentRef)

    def test_options_is_instrument(self):
        ref = OptionsRef(
            underlying="AAPL", expiry=date(2025, 12, 19),
            strike_cents=20000, right="call", style="american",
            multiplier=100, deliverable="AAPL", occ_symbol="AAPL251219C00200000",
        )
        assert isinstance(ref, InstrumentRef)

    def test_can_hold_both_in_collection(self):
        eq = EquityRef(ticker="AAPL")
        opt = OptionsRef(
            underlying="AAPL", expiry=date(2025, 12, 19),
            strike_cents=20000, right="call", style="american",
            multiplier=100, deliverable="AAPL", occ_symbol="AAPL251219C00200000",
        )
        refs: list[InstrumentRef] = [eq, opt]
        assert len(refs) == 2
        assert refs[0].asset_class == "equity"
        assert refs[1].asset_class == "option"
