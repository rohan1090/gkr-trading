"""Sample long-options strategy for V2 runtime — OptionsAwareStrategy protocol.

Emits a single buy_to_open TradeIntent for a call option.
Minimal example for paper certification of the options path.
"""
from __future__ import annotations

import time
import uuid
from typing import Optional

from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.market_data import MarketDataEnvelope
from gkr_trading.core.options_intents import TradeIntent


class SampleLongCallStrategyV2:
    """Buy one call option on any options envelope received.

    Conforms to OptionsAwareStrategy protocol.
    Emits at most one buy_to_open intent, then goes quiet.
    Always uses a limit price (options require limits).
    """

    name = "sample_long_call_v2"

    def __init__(self, session_id: str, *, quantity: int = 1) -> None:
        self._session_id = session_id
        self._quantity = quantity
        self._has_emitted = False

    def on_market_data(
        self,
        envelope: MarketDataEnvelope,
        context: object,
    ) -> Optional[TradeIntent]:
        if self._has_emitted:
            return None

        # Only handle options
        if not isinstance(envelope.instrument_ref, OptionsRef):
            return None

        # Use ask price as limit, fall back to last, then close
        limit = envelope.ask_cents or envelope.last_cents or envelope.close_cents
        if limit is None:
            return None

        self._has_emitted = True
        return TradeIntent(
            intent_id=str(uuid.uuid4()),
            strategy_id=self.name,
            session_id=self._session_id,
            venue_class="traditional",
            instrument_ref=envelope.instrument_ref,
            action="buy_to_open",
            quantity=self._quantity,
            limit_price_cents=limit,
            time_in_force="day",
            created_at_ns=time.time_ns(),
        )
