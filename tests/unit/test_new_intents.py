"""Tests for options-capable TradeIntent and SpreadIntent."""
from __future__ import annotations

from datetime import date

import pytest

from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.core.options_intents import SpreadIntent, SpreadLeg, TradeIntent


def _equity_ref() -> EquityRef:
    return EquityRef(ticker="AAPL")


def _options_ref() -> OptionsRef:
    return OptionsRef(
        underlying="AAPL", expiry=date(2025, 12, 19),
        strike_cents=20000, right="call", style="american",
        multiplier=100, deliverable="AAPL", occ_symbol="AAPL251219C00200000",
    )


class TestTradeIntent:
    def test_equity_market_order(self):
        intent = TradeIntent(
            intent_id="i1", strategy_id="s1", session_id="sess1",
            venue_class="traditional", instrument_ref=_equity_ref(),
            action="buy_to_open", quantity=100,
            limit_price_cents=None, time_in_force="day",
            created_at_ns=1000,
        )
        assert intent.quantity == 100
        assert intent.action == "buy_to_open"

    def test_equity_limit_order(self):
        intent = TradeIntent(
            intent_id="i1", strategy_id="s1", session_id="sess1",
            venue_class="traditional", instrument_ref=_equity_ref(),
            action="sell_to_close", quantity=50,
            limit_price_cents=15000, time_in_force="gtc",
            created_at_ns=1000,
        )
        assert intent.limit_price_cents == 15000

    def test_options_require_limit_price(self):
        """Options must have a limit price — market orders forbidden."""
        with pytest.raises(ValueError, match="options require a limit price"):
            TradeIntent(
                intent_id="i1", strategy_id="s1", session_id="sess1",
                venue_class="traditional", instrument_ref=_options_ref(),
                action="buy_to_open", quantity=1,
                limit_price_cents=None,  # <-- forbidden for options
                time_in_force="day", created_at_ns=1000,
            )

    def test_options_with_limit_price_ok(self):
        intent = TradeIntent(
            intent_id="i1", strategy_id="s1", session_id="sess1",
            venue_class="traditional", instrument_ref=_options_ref(),
            action="buy_to_open", quantity=5,
            limit_price_cents=350, time_in_force="day",
            created_at_ns=1000,
        )
        assert intent.quantity == 5
        assert intent.limit_price_cents == 350

    def test_zero_quantity_rejected(self):
        with pytest.raises(ValueError, match="quantity must be positive"):
            TradeIntent(
                intent_id="i1", strategy_id="s1", session_id="sess1",
                venue_class="traditional", instrument_ref=_equity_ref(),
                action="buy_to_open", quantity=0,
                limit_price_cents=None, time_in_force="day",
                created_at_ns=1000,
            )

    def test_negative_limit_price_rejected(self):
        with pytest.raises(ValueError, match="limit_price_cents must be positive"):
            TradeIntent(
                intent_id="i1", strategy_id="s1", session_id="sess1",
                venue_class="traditional", instrument_ref=_equity_ref(),
                action="buy_to_open", quantity=100,
                limit_price_cents=-100, time_in_force="day",
                created_at_ns=1000,
            )

    def test_frozen(self):
        intent = TradeIntent(
            intent_id="i1", strategy_id="s1", session_id="sess1",
            venue_class="traditional", instrument_ref=_equity_ref(),
            action="buy_to_open", quantity=100,
            limit_price_cents=None, time_in_force="day",
            created_at_ns=1000,
        )
        with pytest.raises(AttributeError):
            intent.quantity = 200  # type: ignore[misc]


class TestSpreadIntent:
    def _leg(self, right: str = "call", strike: int = 20000, action: str = "buy_to_open") -> SpreadLeg:
        ref = OptionsRef(
            underlying="AAPL", expiry=date(2025, 12, 19),
            strike_cents=strike, right=right, style="american",
            multiplier=100, deliverable="AAPL",
            occ_symbol=f"AAPL251219{'C' if right == 'call' else 'P'}{strike * 10:08d}",
        )
        return SpreadLeg(instrument_ref=ref, action=action, ratio_quantity=1)

    def test_valid_spread(self):
        spread = SpreadIntent(
            intent_id="s1", strategy_id="strat", session_id="sess",
            venue_class="traditional",
            legs=(self._leg(strike=20000, action="buy_to_open"),
                  self._leg(strike=21000, action="sell_to_open")),
            net_limit_price_cents=-50, time_in_force="day",
            created_at_ns=1000,
        )
        assert len(spread.legs) == 2

    def test_single_leg_rejected(self):
        with pytest.raises(ValueError, match="requires at least 2 legs"):
            SpreadIntent(
                intent_id="s1", strategy_id="strat", session_id="sess",
                venue_class="traditional",
                legs=(self._leg(),),
                net_limit_price_cents=100, time_in_force="day",
                created_at_ns=1000,
            )

    def test_different_underlyings_rejected(self):
        leg1 = self._leg()
        ref2 = OptionsRef(
            underlying="GOOG", expiry=date(2025, 12, 19),
            strike_cents=20000, right="call", style="american",
            multiplier=100, deliverable="GOOG", occ_symbol="GOOG251219C00200000",
        )
        leg2 = SpreadLeg(instrument_ref=ref2, action="sell_to_open", ratio_quantity=1)
        with pytest.raises(ValueError, match="same underlying"):
            SpreadIntent(
                intent_id="s1", strategy_id="strat", session_id="sess",
                venue_class="traditional",
                legs=(leg1, leg2),
                net_limit_price_cents=100, time_in_force="day",
                created_at_ns=1000,
            )
