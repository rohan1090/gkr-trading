"""ReconciliationService — startup/shutdown/on-demand position reconciliation.

Compares local position state (from PositionStore) against venue-reported positions.
Returns ReconciliationBreak list with severity.
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import List, Tuple

logger = logging.getLogger(__name__)

from gkr_trading.core.position_model import EquityPositionRecord, OptionsContractRecord
from gkr_trading.core.reconciliation_model import (
    OptionsReconciliationSnapshot,
    ReconciliationBreak,
)
from gkr_trading.live.base import VenueAdapter, VenuePosition
from gkr_trading.persistence.position_store import PositionStore


class ReconciliationService:
    """Reconcile local positions against venue-reported positions."""

    def __init__(
        self,
        position_store: PositionStore,
        adapter: VenueAdapter,
        session_id: str,
    ) -> None:
        self._store = position_store
        self._adapter = adapter
        self._session_id = session_id

    def reconcile(
        self,
        trigger: str = "on_demand",
    ) -> OptionsReconciliationSnapshot:
        """Run a full reconciliation. Returns snapshot with any breaks."""
        venue = self._adapter.venue_name
        venue_positions = self._adapter.get_positions()
        venue_account = self._adapter.get_account()

        local_equity = self._store.get_equity_positions(self._session_id, venue)
        local_options = self._store.get_options_positions(self._session_id, venue)

        breaks: List[ReconciliationBreak] = []

        # Reconcile equity positions
        venue_equity_map = {}
        for vp in venue_positions:
            if vp.instrument_key.startswith("equity:"):
                ticker = vp.instrument_key.removeprefix("equity:")
                venue_equity_map[ticker] = vp.quantity

        local_equity_map = {p["ticker"]: p["signed_qty"] for p in local_equity}

        all_tickers = set(venue_equity_map.keys()) | set(local_equity_map.keys())
        for ticker in all_tickers:
            local_qty = local_equity_map.get(ticker, 0)
            venue_qty = venue_equity_map.get(ticker, 0)
            if local_qty != venue_qty:
                breaks.append(ReconciliationBreak(
                    field=f"equity_position:{ticker}",
                    local_value=str(local_qty),
                    venue_value=str(venue_qty),
                    break_type="position",
                    severity="blocking",
                ))

        # Reconcile options positions
        venue_options_map = {}
        for vp in venue_positions:
            if vp.instrument_key.startswith("option:"):
                occ = vp.instrument_key.removeprefix("option:")
                venue_options_map[occ] = vp.quantity

        local_options_map = {}
        for p in local_options:
            net = p["long_contracts"] - p["short_contracts"]
            if net != 0:
                local_options_map[p["occ_symbol"]] = net

        all_occ = set(venue_options_map.keys()) | set(local_options_map.keys())
        for occ in all_occ:
            local_qty = local_options_map.get(occ, 0)
            venue_qty = venue_options_map.get(occ, 0)
            if local_qty != venue_qty:
                breaks.append(ReconciliationBreak(
                    field=f"options_position:{occ}",
                    local_value=str(local_qty),
                    venue_value=str(venue_qty),
                    break_type="position",
                    severity="blocking",
                ))

        # Cash reconciliation
        local_cash = sum(p.get("cost_basis_cents", 0) for p in local_equity)
        venue_cash = venue_account.cash_cents
        # Cash breaks are warnings, not blocking
        if local_cash != venue_cash:
            breaks.append(ReconciliationBreak(
                field="cash_balance",
                local_value=str(local_cash),
                venue_value=str(venue_cash),
                break_type="cash",
                severity="warning",
            ))

        # Orphan / open order detection
        if hasattr(self._adapter, "get_open_orders"):
            try:
                open_orders = self._adapter.get_open_orders()
                for order in open_orders:
                    coid = order.get("client_order_id", "")
                    if coid:
                        breaks.append(ReconciliationBreak(
                            field=f"orphan_order:{coid}",
                            local_value="unknown",
                            venue_value=order.get("status", "open"),
                            break_type="orphan_order",
                            severity="warning",
                        ))
            except Exception as exc:
                logger.warning(f"Open order check failed: {exc}")

        status = "clean" if not breaks else "break_detected"

        return OptionsReconciliationSnapshot(
            snapshot_id=str(uuid.uuid4()),
            session_id=self._session_id,
            timestamp_ns=time.time_ns(),
            trigger=trigger,  # type: ignore[arg-type]
            local_equity_positions=tuple(
                EquityPositionRecord(
                    ticker=p["ticker"], venue=venue, signed_qty=p["signed_qty"],
                    cost_basis_cents=p["cost_basis_cents"],
                    realized_pnl_cents=p["realized_pnl_cents"],
                    status=p["status"] if p["signed_qty"] != 0 else "closed",
                )
                for p in local_equity
            ),
            venue_equity_positions=(),  # populated from venue in full impl
            local_options_positions=(),  # populated from store in full impl
            venue_options_positions=(),
            pending_assignments=(),
            pending_expirations=(),
            local_cash_cents=local_cash,
            venue_cash_cents=venue_cash,
            local_options_buying_power_cents=0,
            venue_options_buying_power_cents=venue_account.options_buying_power_cents,
            local_margin_requirement_cents=0,
            venue_margin_requirement_cents=venue_account.margin_requirement_cents,
            breaks=tuple(breaks),
            status=status,  # type: ignore[arg-type]
        )
