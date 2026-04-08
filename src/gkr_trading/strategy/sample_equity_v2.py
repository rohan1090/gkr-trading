"""Sample equity strategy for V2 runtime — OptionsAwareStrategy protocol.

Emits a TradeIntent (from core.options_intents) to buy AAPL if close < previous close.
Minimal example for paper certification.
"""
from __future__ import annotations

import time
import uuid
from typing import Optional

from gkr_trading.core.instruments import EquityRef
from gkr_trading.core.market_data import MarketDataEnvelope
from gkr_trading.core.options_intents import TradeIntent


class SampleEquityStrategyV2:
    """Buy-the-dip equity strategy on AAPL.

    Conforms to OptionsAwareStrategy protocol.
    Emits at most one buy intent, then goes quiet.
    """

    name = "sample_equity_v2"

    def __init__(self, session_id: str, *, quantity: int = 10) -> None:
        self._session_id = session_id
        self._quantity = quantity
        self._has_emitted = False
        self._prev_close: Optional[int] = None

    def on_market_data(
        self,
        envelope: MarketDataEnvelope,
        context: object,
    ) -> Optional[TradeIntent]:
        if self._has_emitted:
            return None

        # Only handle equities
        if not isinstance(envelope.instrument_ref, EquityRef):
            return None

        close = envelope.close_cents or envelope.last_cents
        if close is None:
            return None

        prev = self._prev_close
        self._prev_close = close

        if prev is not None and close < prev:
            self._has_emitted = True
            return TradeIntent(
                intent_id=str(uuid.uuid4()),
                strategy_id=self.name,
                session_id=self._session_id,
                venue_class="traditional",
                instrument_ref=envelope.instrument_ref,
                action="buy_to_open",
                quantity=self._quantity,
                limit_price_cents=None,  # market order for equities
                time_in_force="day",
                created_at_ns=time.time_ns(),
            )

        return None
