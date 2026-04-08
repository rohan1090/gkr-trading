"""Alpaca options adapter — single-leg options support.

Implements OptionsCapableAdapterMixin for Alpaca.
Uses action semantics (buy_to_open, sell_to_close, etc.) via position_intent.
Options orders REQUIRE limit prices.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.options_lifecycle import AssignmentEvent, ExerciseEvent, ExpirationEvent
from gkr_trading.live.base import SubmissionRequest, SubmissionResponse
from gkr_trading.live.traditional.options.options_adapter_base import OptionsCapableAdapterMixin

logger = logging.getLogger(__name__)


class AlpacaOptionsAdapter(OptionsCapableAdapterMixin):
    """Alpaca options adapter for single-leg options trading.

    Key Alpaca-specific details:
    - Options use position_intent: "buy_to_open", "sell_to_close", etc.
    - Options must be limit orders
    - OCC symbol format for Alpaca API
    - NTA (Non-Trade Activities) for assignment/exercise/expiration
    """

    def __init__(self, http_client: Any, session_id: str = "") -> None:
        self._http = http_client
        self._session_id = session_id

    def submit_options_order(self, request: SubmissionRequest) -> SubmissionResponse:
        """Submit a single-leg options order to Alpaca."""
        ref = request.instrument_ref
        if not isinstance(ref, OptionsRef):
            return SubmissionResponse(
                client_order_id=request.client_order_id,
                venue_order_id=None,
                success=False,
                rejected=True,
                reject_reason="AlpacaOptionsAdapter requires OptionsRef",
            )

        if request.limit_price_cents is None:
            return SubmissionResponse(
                client_order_id=request.client_order_id,
                venue_order_id=None,
                success=False,
                rejected=True,
                reject_reason="Options orders must have a limit price",
            )

        body: Dict[str, Any] = {
            "symbol": ref.occ_symbol,
            "qty": str(request.quantity),
            "side": "buy" if request.action.startswith("buy") else "sell",
            "type": "limit",
            "limit_price": str(request.limit_price_cents / 100.0),
            "time_in_force": request.time_in_force,
            "client_order_id": request.client_order_id,
            "order_class": "simple",
            # Alpaca-specific: position_intent maps to action semantics
            "position_intent": request.action,
        }

        try:
            resp = self._http.post("/v2/orders", body)
            venue_id = resp.get("id")
            return SubmissionResponse(
                client_order_id=request.client_order_id,
                venue_order_id=venue_id,
                success=True,
                raw_response=json.dumps(resp),
            )
        except Exception as exc:
            return SubmissionResponse(
                client_order_id=request.client_order_id,
                venue_order_id=None,
                success=False,
                rejected=True,
                reject_reason=str(exc),
            )

    def get_options_positions(self) -> List[dict]:
        """Get current options positions from Alpaca."""
        try:
            positions = self._http.get("/v2/positions")
            result = []
            for pos in positions:
                asset_class = pos.get("asset_class", "")
                if asset_class != "us_option":
                    continue
                result.append({
                    "occ_symbol": pos.get("symbol", ""),
                    "quantity": int(float(pos.get("qty", 0))),
                    "side": pos.get("side", "long"),
                    "avg_entry_price": pos.get("avg_entry_price", "0"),
                    "market_value": pos.get("market_value", "0"),
                    "unrealized_pl": pos.get("unrealized_pl", "0"),
                })
            return result
        except Exception as exc:
            logger.error(f"Failed to get options positions: {exc}")
            return []

    def get_pending_assignments(self) -> List[str]:
        """Check for pending assignment notifications via NTA activities."""
        # Alpaca NTA activities endpoint for assignment events
        # In paper mode, assignments may be simulated
        return []  # Stub — real impl queries /v2/account/activities/OASGN

    def get_expiring_today(self) -> List[str]:
        """Get OCC symbols expiring today."""
        # Would query options positions and check expiry dates
        return []  # Stub — real impl checks position expiry dates

    def translate_assignment(self, venue_event: dict) -> Optional[AssignmentEvent]:
        """Translate Alpaca NTA assignment event to canonical AssignmentEvent."""
        try:
            symbol = venue_event.get("symbol", "")
            qty = abs(int(float(venue_event.get("qty", 0))))
            # Parse OCC symbol for structured fields
            from gkr_trading.live.traditional.options.options_domain import OCCSymbolParser
            ref = OCCSymbolParser.parse(symbol)

            if ref.right == "call":
                equity_delta = -qty * ref.multiplier  # must deliver shares
            else:
                equity_delta = qty * ref.multiplier   # must buy shares

            return AssignmentEvent(
                event_id=str(uuid.uuid4()),
                session_id=self._session_id,
                seq_no=0,  # assigned by caller
                instrument_ref=ref,
                venue="alpaca_paper",
                contracts_assigned=qty,
                strike_cents=ref.strike_cents,
                right=ref.right,
                resulting_equity_delta=equity_delta,
                equity_underlying=ref.underlying,
                assignment_price_cents=ref.strike_cents,
                effective_date=venue_event.get("date", ""),
                source="auto",
                timestamp_ns=time.time_ns(),
                requires_operator_review=ref.right == "call",  # naked calls need review
            )
        except Exception as exc:
            logger.error(f"Failed to translate assignment: {exc}")
            return None

    def translate_exercise(self, venue_event: dict) -> Optional[ExerciseEvent]:
        """Translate Alpaca NTA exercise event to canonical ExerciseEvent."""
        try:
            symbol = venue_event.get("symbol", "")
            qty = abs(int(float(venue_event.get("qty", 0))))
            from gkr_trading.live.traditional.options.options_domain import OCCSymbolParser
            ref = OCCSymbolParser.parse(symbol)

            if ref.right == "call":
                equity_delta = qty * ref.multiplier
            else:
                equity_delta = -qty * ref.multiplier

            return ExerciseEvent(
                event_id=str(uuid.uuid4()),
                session_id=self._session_id,
                seq_no=0,
                instrument_ref=ref,
                venue="alpaca_paper",
                contracts_exercised=qty,
                strike_cents=ref.strike_cents,
                right=ref.right,
                resulting_equity_delta=equity_delta,
                equity_underlying=ref.underlying,
                effective_date=venue_event.get("date", ""),
                initiated_by="system",
                timestamp_ns=time.time_ns(),
            )
        except Exception as exc:
            logger.error(f"Failed to translate exercise: {exc}")
            return None

    def translate_expiration(self, venue_event: dict) -> Optional[ExpirationEvent]:
        """Translate Alpaca NTA expiration event to canonical ExpirationEvent."""
        try:
            symbol = venue_event.get("symbol", "")
            qty = abs(int(float(venue_event.get("qty", 0))))
            from gkr_trading.live.traditional.options.options_domain import OCCSymbolParser
            ref = OCCSymbolParser.parse(symbol)

            return ExpirationEvent(
                event_id=str(uuid.uuid4()),
                session_id=self._session_id,
                seq_no=0,
                instrument_ref=ref,
                venue="alpaca_paper",
                contracts_expired=qty,
                moneyness_at_expiry="otm",
                premium_paid_cents=0,
                premium_received_cents=0,
                expired_at_ns=time.time_ns(),
                expiry_type="standard_monthly",
            )
        except Exception as exc:
            logger.error(f"Failed to translate expiration: {exc}")
            return None
