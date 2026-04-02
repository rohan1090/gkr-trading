from __future__ import annotations

from typing import Protocol

from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.data.market_store.repository import BarRow


class Strategy(Protocol):
    name: str

    def on_bar(self, bar: BarRow, history: tuple[BarRow, ...]) -> TradeIntent | None: ...
