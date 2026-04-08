from __future__ import annotations

from typing import Protocol

from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.data.market_store.repository import BarRow


class Strategy(Protocol):
    """Legacy equity-only strategy protocol. Preserved for backward compatibility."""
    name: str

    def on_bar(self, bar: BarRow, history: tuple[BarRow, ...]) -> TradeIntent | None: ...


class OptionsAwareStrategy(Protocol):
    """Options-capable strategy protocol.

    Receives MarketDataEnvelope, emits new TradeIntent (from core.options_intents).
    Strategy must NOT touch venues, orders, positions, persistence, EventStore,
    SessionSupervisor, adapters, or broker APIs.
    Strategy must NOT have side effects.
    """
    name: str

    def on_market_data(
        self,
        envelope: object,  # MarketDataEnvelope from core.market_data
        context: object,   # read-only session context
    ) -> object | None:  # TradeIntent from core.options_intents
        ...
