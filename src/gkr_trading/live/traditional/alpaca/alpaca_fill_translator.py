"""Alpaca fill translator — venue fills to canonical FillEvent."""
from __future__ import annotations

import uuid
from typing import Optional

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import EquityRef
from gkr_trading.live.fill_translator import FillTranslator


class AlpacaFillTranslator(FillTranslator):
    """Translate Alpaca fill activity payloads into canonical FillEvent."""

    def __init__(self, session_id: str) -> None:
        self._session_id = session_id

    def translate_fill(self, venue_payload: dict) -> Optional[FillEvent]:
        """Translate Alpaca fill payload.

        Expects payload from Alpaca's /v2/account/activities/FILL endpoint
        or from trade_updates websocket.
        """
        activity_type = venue_payload.get("activity_type", venue_payload.get("type", ""))
        if activity_type not in ("FILL", "fill", "partial_fill"):
            return None

        symbol = venue_payload.get("symbol", "")
        qty = abs(int(float(venue_payload.get("qty", 0))))
        price_str = venue_payload.get("price", "0")
        price_cents = int(float(price_str) * 100)
        side = venue_payload.get("side", "buy")
        order_id = venue_payload.get("order_id", venue_payload.get("client_order_id", ""))

        # Map Alpaca side to action (simplified for equities)
        action = "buy_to_open" if side == "buy" else "sell_to_close"

        return FillEvent(
            event_id=str(uuid.uuid4()),
            session_id=self._session_id,
            seq_no=0,  # assigned by caller
            client_order_id=order_id,
            venue_fill_id=venue_payload.get("id", str(uuid.uuid4())),
            instrument_ref=EquityRef(ticker=symbol),
            venue="alpaca_paper",
            action=action,
            quantity=qty,
            price_cents=price_cents,
            fee_cents=0,  # Alpaca paper: no fees
            is_taker=True,
            timestamp_ns=0,
        )
