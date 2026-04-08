"""Alpaca options fill translator — NTA events to lifecycle events.

Translates Alpaca Non-Trade Activities (NTA) for assignment, exercise,
and expiration into canonical options lifecycle events.

These are NOT fills. They are translated via the AlpacaOptionsAdapter.
"""
from __future__ import annotations

import uuid
from typing import Optional

from gkr_trading.core.fills import FillEvent
from gkr_trading.core.instruments import OptionsRef
from gkr_trading.live.fill_translator import FillTranslator
from gkr_trading.live.traditional.options.options_domain import OCCSymbolParser


class AlpacaOptionsFillTranslator(FillTranslator):
    """Translate Alpaca options trade fills (not NTA lifecycle events).

    NTA events (assignment, exercise, expiration) are handled by
    AlpacaOptionsAdapter, not by this translator.
    """

    def __init__(self, session_id: str) -> None:
        self._session_id = session_id

    def translate_fill(self, venue_payload: dict) -> Optional[FillEvent]:
        """Translate Alpaca options fill payload."""
        activity_type = venue_payload.get("activity_type", venue_payload.get("type", ""))
        if activity_type not in ("FILL", "fill", "partial_fill"):
            return None

        symbol = venue_payload.get("symbol", "")
        # Check if this is an options symbol (OCC format)
        asset_class = venue_payload.get("asset_class", "")
        if asset_class != "us_option":
            return None

        try:
            ref = OCCSymbolParser.parse(symbol)
        except Exception:
            return None

        qty = abs(int(float(venue_payload.get("qty", 0))))
        price_str = venue_payload.get("price", "0")
        price_cents = int(float(price_str) * 100)
        order_id = venue_payload.get("order_id", venue_payload.get("client_order_id", ""))

        # Use position_intent from Alpaca if available, else infer from side
        position_intent = venue_payload.get("position_intent", "")
        if not position_intent:
            side = venue_payload.get("side", "buy")
            position_intent = "buy_to_open" if side == "buy" else "sell_to_close"

        return FillEvent(
            event_id=str(uuid.uuid4()),
            session_id=self._session_id,
            seq_no=0,
            client_order_id=order_id,
            venue_fill_id=venue_payload.get("id", str(uuid.uuid4())),
            instrument_ref=ref,
            venue="alpaca_paper",
            action=position_intent,
            quantity=qty,
            price_cents=price_cents,
            fee_cents=0,
            is_taker=True,
            timestamp_ns=0,
        )


def is_nta_lifecycle_event(venue_payload: dict) -> bool:
    """Check if an Alpaca activity is an NTA lifecycle event (not a fill).

    NTA types: OASGN (assignment), OEXC (exercise), OEXP (expiration)
    """
    activity_type = venue_payload.get("activity_type", "")
    return activity_type in ("OASGN", "OEXC", "OEXP")


def get_nta_event_type(venue_payload: dict) -> Optional[str]:
    """Return the lifecycle event type for NTA activities."""
    activity_type = venue_payload.get("activity_type", "")
    mapping = {
        "OASGN": "assignment",
        "OEXC": "exercise",
        "OEXP": "expiration",
    }
    return mapping.get(activity_type)
