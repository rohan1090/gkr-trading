"""Multi-cycle equity strategy for V2 continuous runtime.

Unlike SampleEquityStrategyV2 which emits once then goes quiet, this strategy
emits buy intents on every dip cycle.  Designed for continuous session testing.

Conforms to OptionsAwareStrategy protocol.
"""
from __future__ import annotations

import time
import uuid
from typing import Optional

from gkr_trading.core.instruments import EquityRef
from gkr_trading.core.market_data import MarketDataEnvelope
from gkr_trading.core.options_intents import TradeIntent


class MultiCycleEquityStrategyV2:
    """Buy-the-dip equity strategy that trades on every dip cycle.

    Emits a buy intent every time close drops below previous close,
    with a configurable cooldown between trades.
    """

    name = "multicycle_equity_v2"

    def __init__(
        self,
        session_id: str,
        *,
        quantity: int = 10,
        cooldown_cycles: int = 0,
    ) -> None:
        self._session_id = session_id
        self._quantity = quantity
        self._cooldown_cycles = cooldown_cycles
        self._prev_close: Optional[int] = None
        self._cycles_since_trade = cooldown_cycles + 1  # allow first trade immediately
        self._trade_count = 0

    @property
    def trade_count(self) -> int:
        return self._trade_count

    def on_market_data(
        self,
        envelope: MarketDataEnvelope,
        context: object,
    ) -> Optional[TradeIntent]:
        # Only handle equities
        if not isinstance(envelope.instrument_ref, EquityRef):
            return None

        close = envelope.close_cents or envelope.last_cents
        if close is None:
            return None

        prev = self._prev_close
        self._prev_close = close
        self._cycles_since_trade += 1

        if prev is not None and close < prev:
            if self._cycles_since_trade > self._cooldown_cycles:
                self._cycles_since_trade = 0
                self._trade_count += 1
                return TradeIntent(
                    intent_id=str(uuid.uuid4()),
                    strategy_id=self.name,
                    session_id=self._session_id,
                    venue_class="traditional",
                    instrument_ref=envelope.instrument_ref,
                    action="buy_to_open",
                    quantity=self._quantity,
                    limit_price_cents=None,
                    time_in_force="day",
                    created_at_ns=time.time_ns(),
                )

        return None
