"""Options position accounting — fill to position, handle long/short correctly.

Key invariant: long_contracts and short_contracts are both >= 0.
A position cannot be simultaneously long and short.
"""
from __future__ import annotations

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.position_model import OptionsContractRecord


def apply_options_fill(
    current: OptionsContractRecord | None,
    fill: FillEvent,
) -> OptionsContractRecord:
    """Apply an options fill to produce updated position record."""
    ref = fill.instrument_ref
    assert isinstance(ref, OptionsRef)

    long_c = current.long_contracts if current else 0
    short_c = current.short_contracts if current else 0
    long_prem = current.long_premium_paid_cents if current else 0
    short_prem = current.short_premium_received_cents if current else 0
    rpnl = current.realized_pnl_cents if current else 0
    venue = current.venue if current else fill.venue

    total_premium = fill.quantity * fill.price_cents * ref.multiplier

    if fill.action == "buy_to_open":
        long_c += fill.quantity
        long_prem += total_premium
    elif fill.action == "sell_to_close":
        closing = min(fill.quantity, long_c)
        if closing > 0 and long_c > 0:
            avg_prem_per_contract = long_prem // long_c
            close_prem = fill.quantity * fill.price_cents * ref.multiplier
            rpnl += close_prem - (avg_prem_per_contract * closing)
        long_c = max(0, long_c - fill.quantity)
    elif fill.action == "sell_to_open":
        short_c += fill.quantity
        short_prem += total_premium
    elif fill.action == "buy_to_close":
        closing = min(fill.quantity, short_c)
        if closing > 0 and short_c > 0:
            avg_prem_per_contract = short_prem // short_c
            close_cost = fill.quantity * fill.price_cents * ref.multiplier
            rpnl += (avg_prem_per_contract * closing) - close_cost
        short_c = max(0, short_c - fill.quantity)

    rpnl -= fill.fee_cents

    # Naked short call = undefined risk
    has_undefined = short_c > 0 and ref.right == "call"

    if long_c == 0 and short_c == 0:
        status = "closed"
    else:
        status = "open"

    return OptionsContractRecord(
        instrument_ref=ref,
        venue=venue,
        long_contracts=long_c,
        short_contracts=short_c,
        long_premium_paid_cents=long_prem,
        short_premium_received_cents=short_prem,
        realized_pnl_cents=rpnl,
        status=status,
        has_undefined_risk=has_undefined,
    )
