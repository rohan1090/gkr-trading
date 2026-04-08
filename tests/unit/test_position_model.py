"""Tests for position model — OptionsContractRecord invariants."""
from __future__ import annotations

from datetime import date

import pytest

from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.position_model import EquityPositionRecord, OptionsContractRecord


def _ref() -> OptionsRef:
    return OptionsRef(
        underlying="AAPL", expiry=date(2025, 12, 19),
        strike_cents=20000, right="call", style="american",
        multiplier=100, deliverable="AAPL", occ_symbol="AAPL251219C00200000",
    )


class TestOptionsContractRecord:
    def test_long_only(self):
        rec = OptionsContractRecord(
            instrument_ref=_ref(), venue="alpaca",
            long_contracts=5, short_contracts=0,
            long_premium_paid_cents=25000, short_premium_received_cents=0,
            realized_pnl_cents=0, status="open", has_undefined_risk=False,
        )
        assert rec.net_contracts == 5
        assert not rec.is_flat

    def test_short_only(self):
        rec = OptionsContractRecord(
            instrument_ref=_ref(), venue="alpaca",
            long_contracts=0, short_contracts=3,
            long_premium_paid_cents=0, short_premium_received_cents=15000,
            realized_pnl_cents=0, status="open", has_undefined_risk=True,
        )
        assert rec.net_contracts == -3
        assert not rec.is_flat

    def test_simultaneous_long_and_short_rejected(self):
        """A position cannot be simultaneously long and short the same contract."""
        with pytest.raises(ValueError, match="simultaneous long and short"):
            OptionsContractRecord(
                instrument_ref=_ref(), venue="alpaca",
                long_contracts=3, short_contracts=3,
                long_premium_paid_cents=15000, short_premium_received_cents=15000,
                realized_pnl_cents=0, status="open", has_undefined_risk=False,
            )

    def test_negative_long_rejected(self):
        with pytest.raises(ValueError, match="long_contracts must be >= 0"):
            OptionsContractRecord(
                instrument_ref=_ref(), venue="alpaca",
                long_contracts=-1, short_contracts=0,
                long_premium_paid_cents=0, short_premium_received_cents=0,
                realized_pnl_cents=0, status="open", has_undefined_risk=False,
            )

    def test_negative_short_rejected(self):
        with pytest.raises(ValueError, match="short_contracts must be >= 0"):
            OptionsContractRecord(
                instrument_ref=_ref(), venue="alpaca",
                long_contracts=0, short_contracts=-1,
                long_premium_paid_cents=0, short_premium_received_cents=0,
                realized_pnl_cents=0, status="open", has_undefined_risk=False,
            )

    def test_flat_position(self):
        rec = OptionsContractRecord(
            instrument_ref=_ref(), venue="alpaca",
            long_contracts=0, short_contracts=0,
            long_premium_paid_cents=0, short_premium_received_cents=0,
            realized_pnl_cents=500, status="closed", has_undefined_risk=False,
        )
        assert rec.is_flat
        assert rec.net_contracts == 0


class TestEquityPositionRecord:
    def test_open_zero_qty_rejected(self):
        with pytest.raises(ValueError, match="open position must have non-zero"):
            EquityPositionRecord(
                ticker="AAPL", venue="alpaca",
                signed_qty=0, cost_basis_cents=0,
                realized_pnl_cents=0, status="open",
            )

    def test_closed_zero_qty_ok(self):
        rec = EquityPositionRecord(
            ticker="AAPL", venue="alpaca",
            signed_qty=0, cost_basis_cents=0,
            realized_pnl_cents=1000, status="closed",
        )
        assert rec.signed_qty == 0
