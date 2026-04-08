"""Market data envelope — normalized market data container.

Strategy receives MarketDataEnvelope with InstrumentRef, never raw tickers.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from gkr_trading.core.instruments import InstrumentRef


@dataclass(frozen=True)
class MarketDataEnvelope:
    """Normalized market data delivered to strategy.

    Contains instrument reference and price data.
    Strategy uses this — never raw broker payloads.
    """
    instrument_ref: InstrumentRef
    timestamp_ns: int
    open_cents: Optional[int] = None
    high_cents: Optional[int] = None
    low_cents: Optional[int] = None
    close_cents: Optional[int] = None
    volume: Optional[int] = None
    bid_cents: Optional[int] = None
    ask_cents: Optional[int] = None
    last_cents: Optional[int] = None
    # Options-specific greeks (None for equities)
    implied_vol: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    open_interest: Optional[int] = None
