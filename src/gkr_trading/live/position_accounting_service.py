"""PositionAccountingService — dispatches fills and lifecycle events.

Routes FillEvent to equity or options accounting based on instrument_ref.
Routes AssignmentEvent, ExerciseEvent, ExpirationEvent to options handlers.
"""
from __future__ import annotations

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import EquityRef, OptionsRef
from gkr_trading.core.options_lifecycle import AssignmentEvent, ExerciseEvent, ExpirationEvent
from gkr_trading.persistence.position_store import PositionStore


class PositionAccountingService:
    """Dispatches position mutations to the correct accounting module."""

    def __init__(self, position_store: PositionStore, session_id: str) -> None:
        self._store = position_store
        self._session_id = session_id

    def apply_fill(self, fill: FillEvent) -> None:
        """Apply a fill event to the appropriate position model."""
        if isinstance(fill.instrument_ref, EquityRef):
            self._apply_equity_fill(fill)
        elif isinstance(fill.instrument_ref, OptionsRef):
            self._apply_options_fill(fill)

    def apply_assignment(self, event: AssignmentEvent) -> None:
        """Assignment closes options position, creates/modifies equity position."""
        ref = event.instrument_ref
        # Close options position
        self._store.upsert_options(
            occ_symbol=ref.occ_symbol,
            venue=event.venue,
            session_id=self._session_id,
            instrument_ref_json="{}",  # simplified
            long_contracts=0,
            short_contracts=0,
            long_premium_paid_cents=0,
            short_premium_received_cents=0,
            realized_pnl_cents=0,
            status="assigned",
            has_undefined_risk=False,
        )
        # Create/modify equity position
        existing = self._store.get_equity_positions(self._session_id, event.venue)
        current_qty = 0
        current_cost = 0
        for pos in existing:
            if pos["ticker"] == event.equity_underlying:
                current_qty = pos["signed_qty"]
                current_cost = pos["cost_basis_cents"]
                break
        new_qty = current_qty + event.resulting_equity_delta
        cost_delta = abs(event.resulting_equity_delta) * event.assignment_price_cents
        if event.resulting_equity_delta > 0:
            new_cost = current_cost + cost_delta
        else:
            new_cost = current_cost - cost_delta
        self._store.upsert_equity(
            ticker=event.equity_underlying,
            venue=event.venue,
            session_id=self._session_id,
            signed_qty=new_qty,
            cost_basis_cents=new_cost,
            realized_pnl_cents=0,
            status="open" if new_qty != 0 else "closed",
        )

    def apply_exercise(self, event: ExerciseEvent) -> None:
        """Exercise closes options position, creates/modifies equity position."""
        ref = event.instrument_ref
        self._store.upsert_options(
            occ_symbol=ref.occ_symbol,
            venue=event.venue,
            session_id=self._session_id,
            instrument_ref_json="{}",
            long_contracts=0,
            short_contracts=0,
            long_premium_paid_cents=0,
            short_premium_received_cents=0,
            realized_pnl_cents=0,
            status="exercised",
            has_undefined_risk=False,
        )
        existing = self._store.get_equity_positions(self._session_id, event.venue)
        current_qty = 0
        current_cost = 0
        for pos in existing:
            if pos["ticker"] == event.equity_underlying:
                current_qty = pos["signed_qty"]
                current_cost = pos["cost_basis_cents"]
                break
        new_qty = current_qty + event.resulting_equity_delta
        cost_delta = abs(event.resulting_equity_delta) * event.strike_cents
        new_cost = current_cost + cost_delta if event.resulting_equity_delta > 0 else current_cost - cost_delta
        self._store.upsert_equity(
            ticker=event.equity_underlying,
            venue=event.venue,
            session_id=self._session_id,
            signed_qty=new_qty,
            cost_basis_cents=new_cost,
            realized_pnl_cents=0,
            status="open" if new_qty != 0 else "closed",
        )

    def apply_expiration(self, event: ExpirationEvent) -> None:
        """Expiration removes options position. No equity change. No cash flow."""
        ref = event.instrument_ref
        self._store.upsert_options(
            occ_symbol=ref.occ_symbol,
            venue=event.venue,
            session_id=self._session_id,
            instrument_ref_json="{}",
            long_contracts=0,
            short_contracts=0,
            long_premium_paid_cents=0,
            short_premium_received_cents=0,
            realized_pnl_cents=0,
            status="expired",
            has_undefined_risk=False,
        )

    def _apply_equity_fill(self, fill: FillEvent) -> None:
        """Apply equity fill to position store."""
        ref = fill.instrument_ref
        assert isinstance(ref, EquityRef)
        existing = self._store.get_equity_positions(self._session_id, fill.venue)
        current_qty = 0
        current_cost = 0
        for pos in existing:
            if pos["ticker"] == ref.ticker:
                current_qty = pos["signed_qty"]
                current_cost = pos["cost_basis_cents"]
                break
        if fill.action in ("buy_to_open", "buy_to_close"):
            delta = fill.quantity
        else:
            delta = -fill.quantity
        new_qty = current_qty + delta
        fill_cost = fill.quantity * fill.price_cents
        if delta > 0:
            new_cost = current_cost + fill_cost
        else:
            new_cost = current_cost - fill_cost
        self._store.upsert_equity(
            ticker=ref.ticker,
            venue=fill.venue,
            session_id=self._session_id,
            signed_qty=new_qty,
            cost_basis_cents=new_cost,
            realized_pnl_cents=0,
            status="open" if new_qty != 0 else "closed",
        )

    def _apply_options_fill(self, fill: FillEvent) -> None:
        """Apply options fill to position store."""
        ref = fill.instrument_ref
        assert isinstance(ref, OptionsRef)
        existing = self._store.get_options_positions(self._session_id, fill.venue)
        long_c = 0
        short_c = 0
        long_prem = 0
        short_prem = 0
        for pos in existing:
            if pos["occ_symbol"] == ref.occ_symbol:
                long_c = pos["long_contracts"]
                short_c = pos["short_contracts"]
                long_prem = pos["long_premium_paid_cents"]
                short_prem = pos["short_premium_received_cents"]
                break
        total_premium = fill.quantity * fill.price_cents * ref.multiplier
        if fill.action == "buy_to_open":
            long_c += fill.quantity
            long_prem += total_premium
        elif fill.action == "sell_to_close":
            long_c = max(0, long_c - fill.quantity)
        elif fill.action == "sell_to_open":
            short_c += fill.quantity
            short_prem += total_premium
        elif fill.action == "buy_to_close":
            short_c = max(0, short_c - fill.quantity)
        has_undefined = short_c > 0 and ref.right == "call"
        status = "open" if (long_c > 0 or short_c > 0) else "closed"
        from gkr_trading.live.order_submission_service import _instrument_ref_to_json
        self._store.upsert_options(
            occ_symbol=ref.occ_symbol,
            venue=fill.venue,
            session_id=self._session_id,
            instrument_ref_json=_instrument_ref_to_json(ref),
            long_contracts=long_c,
            short_contracts=short_c,
            long_premium_paid_cents=long_prem,
            short_premium_received_cents=short_prem,
            realized_pnl_cents=0,
            status=status,
            has_undefined_risk=has_undefined,
        )
