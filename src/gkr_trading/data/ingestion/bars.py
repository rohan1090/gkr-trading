from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.schemas.ids import InstrumentId
from gkr_trading.data.market_store.repository import BarRow, MarketDataRepository


def ingest_equity_bars(
    repo: MarketDataRepository,
    instrument_id: InstrumentId,
    timeframe: str,
    bars: list[tuple[str, Decimal, Decimal, Decimal, Decimal, Decimal]],
) -> None:
    """bars: (bar_ts_utc, o, h, l, c, vol)."""
    for ts, o, h, l, c, v in bars:
        repo.insert_equity_bar(
            BarRow(
                instrument_id=instrument_id,
                timeframe=timeframe,
                bar_ts_utc=ts,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v,
            )
        )
