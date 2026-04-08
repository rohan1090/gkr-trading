"""Options reconciler — compare local vs venue options positions."""
from __future__ import annotations

from typing import List

from gkr_trading.core.reconciliation_model import ReconciliationBreak


def reconcile_options_positions(
    local: List[dict],
    venue: List[dict],
) -> List[ReconciliationBreak]:
    """Compare local and venue options positions, return breaks.

    local: list of {"occ_symbol": str, "long_contracts": int, "short_contracts": int}
    venue: list of {"occ_symbol": str, "quantity": int} (signed: positive=long, negative=short)
    """
    local_map = {}
    for p in local:
        net = p.get("long_contracts", 0) - p.get("short_contracts", 0)
        if net != 0:
            local_map[p["occ_symbol"]] = net

    venue_map = {p["occ_symbol"]: p["quantity"] for p in venue if p.get("quantity", 0) != 0}

    breaks: List[ReconciliationBreak] = []
    all_symbols = set(local_map.keys()) | set(venue_map.keys())

    for occ in sorted(all_symbols):
        lq = local_map.get(occ, 0)
        vq = venue_map.get(occ, 0)
        if lq != vq:
            breaks.append(ReconciliationBreak(
                field=f"option:{occ}",
                local_value=str(lq),
                venue_value=str(vq),
                break_type="position",
                severity="blocking",
            ))

    return breaks
