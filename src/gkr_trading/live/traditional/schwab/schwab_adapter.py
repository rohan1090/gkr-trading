"""Schwab adapter — STUB. Not implemented.

Interface-safe stubs only. No live integration work.
"""
from __future__ import annotations

from typing import List, Optional

from gkr_trading.core.order_model import OrderStatus
from gkr_trading.live.base import (
    SubmissionRequest,
    SubmissionResponse,
    VenueAccountInfo,
    VenueAdapter,
    VenuePosition,
)


class SchwabAdapter(VenueAdapter):
    """STUB: Schwab venue adapter. Not yet implemented."""

    @property
    def venue_name(self) -> str:
        return "schwab"

    def submit_order(self, request: SubmissionRequest) -> SubmissionResponse:
        raise NotImplementedError("SchwabAdapter is not yet implemented")

    def cancel_order(self, client_order_id: str) -> bool:
        raise NotImplementedError("SchwabAdapter is not yet implemented")

    def get_order_status(self, client_order_id: str) -> Optional[OrderStatus]:
        raise NotImplementedError("SchwabAdapter is not yet implemented")

    def get_positions(self) -> List[VenuePosition]:
        raise NotImplementedError("SchwabAdapter is not yet implemented")

    def get_account(self) -> VenueAccountInfo:
        raise NotImplementedError("SchwabAdapter is not yet implemented")
