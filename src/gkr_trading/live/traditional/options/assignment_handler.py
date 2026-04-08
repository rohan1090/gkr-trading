"""Assignment handler — processes AssignmentEvent.

Assignment is NOT a fill. It creates an equity position delta
and simultaneously closes the options position.
"""
from __future__ import annotations

from gkr_trading.core.options_lifecycle import AssignmentEvent
from gkr_trading.core.position_model import EquityPositionRecord, OptionsContractRecord


def process_assignment(
    event: AssignmentEvent,
    current_options: OptionsContractRecord | None,
    current_equity: EquityPositionRecord | None,
) -> tuple[OptionsContractRecord, EquityPositionRecord]:
    """Process an assignment event.

    Returns (updated_options_record, updated_equity_record).
    The options position is closed. The equity position is created/modified.
    """
    ref = event.instrument_ref

    # Close the options position (short contracts assigned away)
    if current_options is not None:
        new_short = max(0, current_options.short_contracts - event.contracts_assigned)
        options_record = OptionsContractRecord(
            instrument_ref=ref,
            venue=event.venue,
            long_contracts=current_options.long_contracts,
            short_contracts=new_short,
            long_premium_paid_cents=current_options.long_premium_paid_cents,
            short_premium_received_cents=current_options.short_premium_received_cents,
            realized_pnl_cents=current_options.realized_pnl_cents,
            status="assigned" if new_short == 0 and current_options.long_contracts == 0 else "open",
            has_undefined_risk=new_short > 0 and ref.right == "call",
        )
    else:
        options_record = OptionsContractRecord(
            instrument_ref=ref,
            venue=event.venue,
            long_contracts=0,
            short_contracts=0,
            long_premium_paid_cents=0,
            short_premium_received_cents=0,
            realized_pnl_cents=0,
            status="assigned",
            has_undefined_risk=False,
        )

    # Create/modify equity position
    eq_qty = current_equity.signed_qty if current_equity else 0
    eq_cost = current_equity.cost_basis_cents if current_equity else 0
    eq_rpnl = current_equity.realized_pnl_cents if current_equity else 0

    new_qty = eq_qty + event.resulting_equity_delta
    cost_delta = abs(event.resulting_equity_delta) * event.assignment_price_cents
    if event.resulting_equity_delta > 0:
        new_cost = eq_cost + cost_delta
    else:
        new_cost = eq_cost - cost_delta

    equity_record = EquityPositionRecord(
        ticker=event.equity_underlying,
        venue=event.venue,
        signed_qty=new_qty,
        cost_basis_cents=new_cost,
        realized_pnl_cents=eq_rpnl,
        status="open" if new_qty != 0 else "closed",
    )

    return options_record, equity_record
