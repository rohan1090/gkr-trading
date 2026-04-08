"""Equity reconciler — compare local vs venue equity positions."""
from __future__ import annotations

from typing import List

from gkr_trading.core.reconciliation_model import ReconciliationBreak


def reconcile_equity_positions(
    local: List[dict],
    venue: List[dict],
) -> List[ReconciliationBreak]:
    """Compare local and venue equity positions, return breaks.

    local: list of {"ticker": str, "signed_qty": int}
    venue: list of {"ticker": str, "signed_qty": int}
    """
    local_map = {p["ticker"]: p["signed_qty"] for p in local if p.get("signed_qty", 0) != 0}
    venue_map = {p["ticker"]: p["signed_qty"] for p in venue if p.get("signed_qty", 0) != 0}

    breaks: List[ReconciliationBreak] = []
    all_tickers = set(local_map.keys()) | set(venue_map.keys())

    for ticker in sorted(all_tickers):
        lq = local_map.get(ticker, 0)
        vq = venue_map.get(ticker, 0)
        if lq != vq:
            breaks.append(ReconciliationBreak(
                field=f"equity:{ticker}",
                local_value=str(lq),
                venue_value=str(vq),
                break_type="position",
                severity="blocking",
            ))

    return breaks
