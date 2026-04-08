"""Alpaca options reconciler — compare local vs Alpaca options positions."""
from __future__ import annotations

from typing import List

from gkr_trading.core.reconciliation_model import ReconciliationBreak
from gkr_trading.live.traditional.options.options_reconciler import reconcile_options_positions


def reconcile_alpaca_options(
    local_positions: List[dict],
    alpaca_positions: List[dict],
) -> List[ReconciliationBreak]:
    """Reconcile local options positions against Alpaca-reported positions.

    alpaca_positions: raw Alpaca /v2/positions response filtered to options.
    """
    venue_normalized = []
    for pos in alpaca_positions:
        asset_class = pos.get("asset_class", "")
        if asset_class != "us_option":
            continue
        qty = int(float(pos.get("qty", 0)))
        side = pos.get("side", "long")
        if side == "short":
            qty = -qty
        venue_normalized.append({
            "occ_symbol": pos.get("symbol", ""),
            "quantity": qty,
        })

    return reconcile_options_positions(local_positions, venue_normalized)
