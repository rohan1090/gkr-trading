"""Equity position accounting — fill to position update."""
from __future__ import annotations

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import EquityRef
from gkr_trading.core.position_model import EquityPositionRecord


def apply_equity_fill(
    current: EquityPositionRecord | None,
    fill: FillEvent,
) -> EquityPositionRecord:
    """Apply an equity fill to produce updated position record."""
    ref = fill.instrument_ref
    assert isinstance(ref, EquityRef)

    current_qty = current.signed_qty if current else 0
    current_cost = current.cost_basis_cents if current else 0
    current_rpnl = current.realized_pnl_cents if current else 0
    venue = current.venue if current else fill.venue

    if fill.action in ("buy_to_open", "buy_to_close"):
        delta = fill.quantity
    else:
        delta = -fill.quantity

    new_qty = current_qty + delta
    fill_cost = fill.quantity * fill.price_cents + fill.fee_cents

    # P&L: realized when closing
    realized_delta = 0
    if current_qty != 0 and ((current_qty > 0 and delta < 0) or (current_qty < 0 and delta > 0)):
        closing_qty = min(abs(delta), abs(current_qty))
        avg_entry = abs(current_cost) // abs(current_qty) if current_qty != 0 else 0
        if current_qty > 0:
            realized_delta = (fill.price_cents - avg_entry) * closing_qty - fill.fee_cents
        else:
            realized_delta = (avg_entry - fill.price_cents) * closing_qty - fill.fee_cents

    if delta > 0:
        new_cost = current_cost + fill_cost
    else:
        if current_qty != 0:
            per_share_cost = current_cost // current_qty if current_qty != 0 else 0
            new_cost = per_share_cost * new_qty
        else:
            new_cost = -fill_cost

    return EquityPositionRecord(
        ticker=ref.ticker,
        venue=venue,
        signed_qty=new_qty,
        cost_basis_cents=new_cost,
        realized_pnl_cents=current_rpnl + realized_delta,
        status="open" if new_qty != 0 else "closed",
    )
