"""Tests for OptionsRiskPolicy — undefined risk, expiry window, assignment hazard."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.live.traditional.options.options_risk_policy import OptionsRiskPolicy


def _options_ref(right="call", expiry_date=None, strike=20000):
    return OptionsRef(
        underlying="AAPL",
        expiry=expiry_date or date(2099, 12, 19),  # far future default
        strike_cents=strike, right=right, style="american",
        multiplier=100, deliverable="AAPL",
        occ_symbol=f"AAPL{(expiry_date or date(2099,12,19)).strftime('%y%m%d')}{'C' if right=='call' else 'P'}{strike*10:08d}",
    )


def _intent(ref, action="buy_to_open", qty=1):
    return TradeIntent(
        intent_id="i1", strategy_id="s1", session_id="sess",
        venue_class="traditional", instrument_ref=ref,
        action=action, quantity=qty,
        limit_price_cents=500, time_in_force="day",
        created_at_ns=1000,
    )


class TestUndefinedRiskBlock:
    def test_naked_short_call_blocked(self):
        policy = OptionsRiskPolicy(block_undefined_risk=True)
        decision = policy.evaluate(
            _intent(_options_ref("call"), action="sell_to_open"),
            context=None,
        )
        assert not decision.approved
        assert decision.reason_code == "UNDEFINED_RISK"

    def test_short_put_allowed(self):
        """Short puts have defined risk (max loss = strike * multiplier)."""
        policy = OptionsRiskPolicy(block_undefined_risk=True)
        decision = policy.evaluate(
            _intent(_options_ref("put"), action="sell_to_open"),
            context=None,
        )
        assert decision.approved

    def test_long_call_allowed(self):
        policy = OptionsRiskPolicy(block_undefined_risk=True)
        decision = policy.evaluate(
            _intent(_options_ref("call"), action="buy_to_open"),
            context=None,
        )
        assert decision.approved

    def test_closing_short_call_allowed(self):
        """Buying to close a short call reduces risk — should be allowed."""
        policy = OptionsRiskPolicy(block_undefined_risk=True)
        decision = policy.evaluate(
            _intent(_options_ref("call"), action="buy_to_close"),
            context=None,
        )
        assert decision.approved


class TestMaxContracts:
    def test_over_max_blocked(self):
        policy = OptionsRiskPolicy(max_contracts=5)
        decision = policy.evaluate(
            _intent(_options_ref(), qty=10),
            context=None,
        )
        assert not decision.approved
        assert decision.reason_code == "MAX_CONTRACTS"

    def test_at_max_allowed(self):
        policy = OptionsRiskPolicy(max_contracts=5)
        decision = policy.evaluate(
            _intent(_options_ref(), qty=5),
            context=None,
        )
        assert decision.approved


class TestExpiryWindowBlock:
    def test_opening_order_near_expiry_blocked(self):
        """Cannot open new positions within expiry window."""
        today = datetime.now(timezone.utc).date()
        policy = OptionsRiskPolicy(expiry_window_days=1)
        decision = policy.evaluate(
            _intent(_options_ref(expiry_date=today), action="buy_to_open"),
            context=None,
        )
        assert not decision.approved
        assert decision.reason_code == "EXPIRY_WINDOW_BLOCK"

    def test_closing_order_near_expiry_allowed(self):
        """Closing existing positions near expiry should be allowed."""
        today = datetime.now(timezone.utc).date()
        policy = OptionsRiskPolicy(expiry_window_days=1)
        decision = policy.evaluate(
            _intent(_options_ref(expiry_date=today), action="sell_to_close"),
            context=None,
        )
        assert decision.approved

    def test_opening_order_far_from_expiry_allowed(self):
        policy = OptionsRiskPolicy(expiry_window_days=1)
        decision = policy.evaluate(
            _intent(_options_ref(expiry_date=date(2099, 12, 19)), action="buy_to_open"),
            context=None,
        )
        assert decision.approved


class TestEquityPassthrough:
    def test_equity_intent_passes_through(self):
        """OptionsRiskPolicy should pass through equity intents."""
        policy = OptionsRiskPolicy()
        intent = TradeIntent(
            intent_id="i1", strategy_id="s1", session_id="sess",
            venue_class="traditional",
            instrument_ref=EquityRef(ticker="AAPL"),
            action="buy_to_open", quantity=100,
            limit_price_cents=None, time_in_force="day",
            created_at_ns=1000,
        )
        decision = policy.evaluate(intent, context=None)
        assert decision.approved
