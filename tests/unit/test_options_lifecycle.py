"""Tests for options lifecycle events — Assignment, Exercise, Expiration.

Key invariant: these are NOT fills. They have fundamentally different
semantics, accounting treatment, and reconciliation behavior.
"""
from __future__ import annotations

from datetime import date

import pytest

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.core.options_lifecycle import AssignmentEvent, ExerciseEvent, ExpirationEvent
from gkr_trading.core.position_model import EquityPositionRecord, OptionsContractRecord
from gkr_trading.live.traditional.options.assignment_handler import process_assignment
from gkr_trading.live.traditional.options.exercise_handler import process_exercise
from gkr_trading.live.traditional.options.expiration_handler import process_expiration


def _options_ref(right: str = "call", strike: int = 20000) -> OptionsRef:
    return OptionsRef(
        underlying="AAPL", expiry=date(2025, 12, 19),
        strike_cents=strike, right=right, style="american",
        multiplier=100, deliverable="AAPL",
        occ_symbol=f"AAPL251219{'C' if right == 'call' else 'P'}{strike * 10:08d}",
    )


class TestAssignmentIsNotFill:
    """Assignment is categorically different from a fill."""

    def test_assignment_type_is_not_fill_type(self):
        """AssignmentEvent is NOT a FillEvent."""
        event = AssignmentEvent(
            event_id="a1", session_id="s1", seq_no=1,
            instrument_ref=_options_ref("put"),
            venue="alpaca", contracts_assigned=1,
            strike_cents=20000, right="put",
            resulting_equity_delta=100,  # put assigned: buy 100 shares
            equity_underlying="AAPL",
            assignment_price_cents=20000,
            effective_date="2025-12-19",
            source="auto", timestamp_ns=1000,
            requires_operator_review=False,
        )
        assert not isinstance(event, FillEvent)

    def test_assignment_creates_equity_position(self):
        """Assignment on short put: account buys shares at strike."""
        ref = _options_ref("put")
        event = AssignmentEvent(
            event_id="a1", session_id="s1", seq_no=1,
            instrument_ref=ref, venue="alpaca",
            contracts_assigned=2, strike_cents=20000, right="put",
            resulting_equity_delta=200,  # 2 contracts * 100 shares
            equity_underlying="AAPL",
            assignment_price_cents=20000,
            effective_date="2025-12-19",
            source="auto", timestamp_ns=1000,
            requires_operator_review=False,
        )
        current_opt = OptionsContractRecord(
            instrument_ref=ref, venue="alpaca",
            long_contracts=0, short_contracts=2,
            long_premium_paid_cents=0, short_premium_received_cents=50000,
            realized_pnl_cents=0, status="open", has_undefined_risk=False,
        )
        opt, eq = process_assignment(event, current_opt, None)
        # Options closed
        assert opt.short_contracts == 0
        assert opt.status == "assigned"
        # Equity position created (bought 200 shares at $200)
        assert eq.signed_qty == 200
        assert eq.cost_basis_cents == 200 * 20000

    def test_assignment_on_short_call_creates_negative_equity(self):
        """Assignment on short call: account must deliver shares."""
        ref = _options_ref("call")
        event = AssignmentEvent(
            event_id="a1", session_id="s1", seq_no=1,
            instrument_ref=ref, venue="alpaca",
            contracts_assigned=1, strike_cents=20000, right="call",
            resulting_equity_delta=-100,  # must deliver 100 shares
            equity_underlying="AAPL",
            assignment_price_cents=20000,
            effective_date="2025-12-19",
            source="manual", timestamp_ns=1000,
            requires_operator_review=True,
        )
        current_opt = OptionsContractRecord(
            instrument_ref=ref, venue="alpaca",
            long_contracts=0, short_contracts=1,
            long_premium_paid_cents=0, short_premium_received_cents=30000,
            realized_pnl_cents=0, status="open", has_undefined_risk=True,
        )
        opt, eq = process_assignment(event, current_opt, None)
        assert opt.status == "assigned"
        assert eq.signed_qty == -100  # short equity


class TestExerciseIsNotFill:
    def test_exercise_type_is_not_fill_type(self):
        event = ExerciseEvent(
            event_id="e1", session_id="s1", seq_no=1,
            instrument_ref=_options_ref("call"),
            venue="alpaca", contracts_exercised=1,
            strike_cents=20000, right="call",
            resulting_equity_delta=100,
            equity_underlying="AAPL",
            effective_date="2025-12-19",
            initiated_by="operator", timestamp_ns=1000,
        )
        assert not isinstance(event, FillEvent)

    def test_exercise_call_creates_long_equity(self):
        """Exercising a long call: receive shares at strike."""
        ref = _options_ref("call")
        event = ExerciseEvent(
            event_id="e1", session_id="s1", seq_no=1,
            instrument_ref=ref, venue="alpaca",
            contracts_exercised=3, strike_cents=20000, right="call",
            resulting_equity_delta=300,
            equity_underlying="AAPL",
            effective_date="2025-12-19",
            initiated_by="system", timestamp_ns=1000,
        )
        current_opt = OptionsContractRecord(
            instrument_ref=ref, venue="alpaca",
            long_contracts=3, short_contracts=0,
            long_premium_paid_cents=45000, short_premium_received_cents=0,
            realized_pnl_cents=0, status="open", has_undefined_risk=False,
        )
        opt, eq = process_exercise(event, current_opt, None)
        assert opt.long_contracts == 0
        assert opt.status == "exercised"
        assert eq.signed_qty == 300
        assert eq.cost_basis_cents == 300 * 20000


class TestExpirationIsNotFill:
    def test_expiration_type_is_not_fill_type(self):
        event = ExpirationEvent(
            event_id="x1", session_id="s1", seq_no=1,
            instrument_ref=_options_ref("call"),
            venue="alpaca", contracts_expired=5,
            moneyness_at_expiry="otm",
            premium_paid_cents=25000,
            premium_received_cents=0,
            expired_at_ns=1000, expiry_type="standard_monthly",
        )
        assert not isinstance(event, FillEvent)

    def test_expiration_removes_position_no_cash_flow(self):
        """Expiration: position removed, premium paid is sunk cost."""
        ref = _options_ref("call")
        event = ExpirationEvent(
            event_id="x1", session_id="s1", seq_no=1,
            instrument_ref=ref, venue="alpaca",
            contracts_expired=2, moneyness_at_expiry="otm",
            premium_paid_cents=10000, premium_received_cents=0,
            expired_at_ns=1000, expiry_type="weekly",
        )
        current = OptionsContractRecord(
            instrument_ref=ref, venue="alpaca",
            long_contracts=2, short_contracts=0,
            long_premium_paid_cents=10000, short_premium_received_cents=0,
            realized_pnl_cents=0, status="open", has_undefined_risk=False,
        )
        result = process_expiration(event, current)
        assert result.long_contracts == 0
        assert result.short_contracts == 0
        assert result.status == "expired"
        # Premium paid is realized loss
        assert result.realized_pnl_cents == -10000

    def test_expiration_short_option_keeps_premium(self):
        """Short option expiring worthless: premium received is realized gain."""
        ref = _options_ref("put")
        event = ExpirationEvent(
            event_id="x1", session_id="s1", seq_no=1,
            instrument_ref=ref, venue="alpaca",
            contracts_expired=1, moneyness_at_expiry="otm",
            premium_paid_cents=0, premium_received_cents=5000,
            expired_at_ns=1000, expiry_type="standard_monthly",
        )
        current = OptionsContractRecord(
            instrument_ref=ref, venue="alpaca",
            long_contracts=0, short_contracts=1,
            long_premium_paid_cents=0, short_premium_received_cents=5000,
            realized_pnl_cents=0, status="open", has_undefined_risk=False,
        )
        result = process_expiration(event, current)
        assert result.status == "expired"
        # Premium received is realized gain
        assert result.realized_pnl_cents == 5000


class TestValidationConstraints:
    def test_assignment_zero_contracts_rejected(self):
        with pytest.raises(ValueError, match="contracts_assigned must be positive"):
            AssignmentEvent(
                event_id="a1", session_id="s1", seq_no=1,
                instrument_ref=_options_ref(), venue="alpaca",
                contracts_assigned=0, strike_cents=20000, right="call",
                resulting_equity_delta=0, equity_underlying="AAPL",
                assignment_price_cents=20000, effective_date="2025-12-19",
                source="auto", timestamp_ns=1000, requires_operator_review=False,
            )

    def test_exercise_zero_contracts_rejected(self):
        with pytest.raises(ValueError, match="contracts_exercised must be positive"):
            ExerciseEvent(
                event_id="e1", session_id="s1", seq_no=1,
                instrument_ref=_options_ref(), venue="alpaca",
                contracts_exercised=0, strike_cents=20000, right="call",
                resulting_equity_delta=0, equity_underlying="AAPL",
                effective_date="2025-12-19", initiated_by="system",
                timestamp_ns=1000,
            )

    def test_expiration_zero_contracts_rejected(self):
        with pytest.raises(ValueError, match="contracts_expired must be positive"):
            ExpirationEvent(
                event_id="x1", session_id="s1", seq_no=1,
                instrument_ref=_options_ref(), venue="alpaca",
                contracts_expired=0, moneyness_at_expiry="otm",
                premium_paid_cents=0, premium_received_cents=0,
                expired_at_ns=1000, expiry_type="standard_monthly",
            )
