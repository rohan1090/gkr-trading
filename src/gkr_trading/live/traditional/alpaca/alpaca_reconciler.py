"""Alpaca equity reconciler — compare local vs Alpaca positions."""
from __future__ import annotations

from typing import List

from gkr_trading.core.reconciliation_model import ReconciliationBreak
from gkr_trading.live.traditional.equity_reconciler import reconcile_equity_positions


def reconcile_alpaca_equity(
    local_positions: List[dict],
    alpaca_positions: List[dict],
) -> List[ReconciliationBreak]:
    """Reconcile local equity positions against Alpaca-reported positions.

    alpaca_positions: raw Alpaca /v2/positions response filtered to equities.
    """
    venue_normalized = []
    for pos in alpaca_positions:
        asset_class = pos.get("asset_class", "us_equity")
        if asset_class not in ("us_equity", ""):
            continue
        qty = int(float(pos.get("qty", 0)))
        side = pos.get("side", "long")
        if side == "short":
            qty = -qty
        venue_normalized.append({
            "ticker": pos.get("symbol", ""),
            "signed_qty": qty,
        })

    return reconcile_equity_positions(local_positions, venue_normalized)
