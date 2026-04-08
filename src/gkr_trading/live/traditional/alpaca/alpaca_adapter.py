"""Alpaca paper equity adapter — implements VenueAdapter for equities.

Wraps the existing AlpacaPaperAdapter's HTTP client for the new architecture.
The old live/alpaca_paper_adapter.py is preserved for backward compatibility.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from gkr_trading.core.instruments import EquityRef, InstrumentRef, OptionsRef
from gkr_trading.core.order_model import OrderStatus
from gkr_trading.live.base import (
    SubmissionRequest,
    SubmissionResponse,
    VenueAccountInfo,
    VenueAdapter,
    VenuePosition,
)

logger = logging.getLogger(__name__)


def _map_alpaca_status(status: str) -> OrderStatus:
    """Map Alpaca order status string to canonical OrderStatus."""
    mapping = {
        "new": OrderStatus.PENDING_NEW,
        "accepted": OrderStatus.RESTING,
        "partially_filled": OrderStatus.PARTIALLY_FILLED,
        "filled": OrderStatus.FILLED,
        "done_for_day": OrderStatus.EXPIRED,
        "canceled": OrderStatus.CANCELED,
        "expired": OrderStatus.EXPIRED,
        "replaced": OrderStatus.CANCELED,
        "pending_new": OrderStatus.PENDING_NEW,
        "pending_cancel": OrderStatus.RESTING,
        "pending_replace": OrderStatus.RESTING,
        "rejected": OrderStatus.REJECTED,
    }
    return mapping.get(status.lower(), OrderStatus.UNKNOWN)


class AlpacaPaperEquityAdapter(VenueAdapter):
    """Alpaca paper trading adapter for equities.

    Uses the existing AlpacaHttpClient under the hood.
    """

    def __init__(self, http_client: Any) -> None:
        """Initialize with an AlpacaHttpClient instance.

        Args:
            http_client: An instance of AlpacaHttpClient or compatible.
        """
        self._http = http_client

    @property
    def venue_name(self) -> str:
        return "alpaca_paper"

    def submit_order(self, request: SubmissionRequest) -> SubmissionResponse:
        """Submit an equity order to Alpaca paper."""
        ref = request.instrument_ref
        if not isinstance(ref, EquityRef):
            return SubmissionResponse(
                client_order_id=request.client_order_id,
                venue_order_id=None,
                success=False,
                rejected=True,
                reject_reason="AlpacaPaperEquityAdapter only handles equity orders",
            )

        body: Dict[str, Any] = {
            "symbol": ref.ticker.upper(),
            "qty": str(request.quantity),
            "side": "buy" if request.action.startswith("buy") else "sell",
            "type": "limit" if request.limit_price_cents else "market",
            "time_in_force": request.time_in_force,
            "client_order_id": request.client_order_id,
        }
        if request.limit_price_cents:
            body["limit_price"] = str(request.limit_price_cents / 100.0)

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

    def cancel_order(self, client_order_id: str) -> bool:
        try:
            self._http.delete(f"/v2/orders:by_client_order_id?client_order_id={client_order_id}")
            return True
        except Exception:
            return False

    def get_order_status(self, client_order_id: str) -> Optional[OrderStatus]:
        try:
            resp = self._http.get(
                f"/v2/orders:by_client_order_id?client_order_id={client_order_id}"
            )
            return _map_alpaca_status(resp.get("status", "unknown"))
        except Exception:
            return None

    def get_positions(self) -> List[VenuePosition]:
        try:
            positions = self._http.get("/v2/positions")
            result = []
            for pos in positions:
                ticker = pos.get("symbol", "")
                qty = int(float(pos.get("qty", 0)))
                side = pos.get("side", "long")
                if side == "short":
                    qty = -qty
                avg_cents = int(float(pos.get("avg_entry_price", 0)) * 100)
                mkt_cents = int(float(pos.get("market_value", 0)) * 100)
                result.append(VenuePosition(
                    instrument_key=f"equity:{ticker}",
                    quantity=qty,
                    avg_entry_price_cents=avg_cents,
                    market_value_cents=mkt_cents,
                ))
            return result
        except Exception as exc:
            logger.error(f"Failed to get positions: {exc}")
            return []

    def get_account(self) -> VenueAccountInfo:
        try:
            acct = self._http.get("/v2/account")
            cash = int(float(acct.get("cash", 0)) * 100)
            bp = int(float(acct.get("buying_power", 0)) * 100)
            return VenueAccountInfo(
                cash_cents=cash,
                buying_power_cents=bp,
                options_buying_power_cents=0,  # Alpaca paper: no options BP
                margin_requirement_cents=0,
            )
        except Exception:
            return VenueAccountInfo(cash_cents=0, buying_power_cents=0)
