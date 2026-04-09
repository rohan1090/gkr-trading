"""Background market data poller — feeds live prices to the TUI.

Runs in a Textual worker thread.  Polls AlpacaMarketDataFeed every 15 s
and posts MarketDataMessage to the app message queue.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence

from textual.message import Message

logger = logging.getLogger(__name__)


# ── Messages posted to Textual queue ────────────────────────────────────


@dataclass
class MarketDataSnapshot:
    """One point-in-time snapshot for a single ticker."""

    ticker: str
    last_cents: Optional[int] = None
    bid_cents: Optional[int] = None
    ask_cents: Optional[int] = None
    open_cents: Optional[int] = None
    high_cents: Optional[int] = None
    low_cents: Optional[int] = None
    close_cents: Optional[int] = None
    volume: Optional[int] = None
    prev_close_cents: Optional[int] = None
    # options greeks
    delta: Optional[float] = None
    theta: Optional[float] = None
    implied_vol: Optional[float] = None


class MarketDataMessage(Message):
    """Posted when new market data arrives."""

    def __init__(self, snapshots: list[MarketDataSnapshot]) -> None:
        super().__init__()
        self.snapshots = snapshots


class MarketStatusMessage(Message):
    """Posted when market open/close state changes."""

    def __init__(self, is_open: bool) -> None:
        super().__init__()
        self.is_open = is_open


class MarketErrorMessage(Message):
    """Posted when market data subsystem has an unrecoverable problem."""

    def __init__(self, error: str) -> None:
        super().__init__()
        self.error = error


# ── Poller logic (runs inside Textual worker) ───────────────────────────


class MarketPoller:
    """Stateful poller — call ``poll_once`` repeatedly from a worker loop."""

    def __init__(
        self,
        equity_tickers: Sequence[str] = ("AAPL", "SPY"),
        poll_interval_sec: float = 15.0,
    ) -> None:
        self._tickers = list(equity_tickers)
        self._interval = poll_interval_sec
        self._feed: Any = None
        self._http: Any = None
        self._metadata: Any = None
        self._available = False
        self._last_market_open: Optional[bool] = None
        self._price_history: dict[str, list[int]] = {}
        self._init()

    def _init(self) -> None:
        try:
            from gkr_trading.live.alpaca_config import AlpacaPaperConfig
            from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient
            from gkr_trading.live.market_data_feed import (
                AlpacaMarketDataFeed,
                MarketDataFeedConfig,
            )
            from gkr_trading.live.market_metadata_provider import (
                AlpacaMarketMetadataProvider,
            )

            cfg = AlpacaPaperConfig.from_env()
            self._http = UrllibAlpacaHttpClient(config=cfg)
            md_config = MarketDataFeedConfig(equity_tickers=tuple(self._tickers))
            self._feed = AlpacaMarketDataFeed(http_client=self._http, config=md_config)
            self._metadata = AlpacaMarketMetadataProvider(self._http)
            self._available = True
        except Exception as exc:
            logger.warning(f"Market data unavailable: {exc}")
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    @property
    def interval(self) -> float:
        return self._interval

    def get_price_history(self, ticker: str) -> list[int]:
        """Return collected close prices for sparkline charting."""
        return list(self._price_history.get(ticker, []))

    def poll_once(self) -> tuple[list[MarketDataSnapshot], Optional[bool]]:
        """Execute one poll cycle.

        Returns (snapshots, market_open_or_None).
        Raises on unrecoverable errors.
        """
        if not self._available:
            return [], None

        snapshots: list[MarketDataSnapshot] = []
        market_open: Optional[bool] = None

        # Check market status
        try:
            market_open = self._metadata.is_market_open()
        except Exception:
            pass

        # Poll market data
        try:
            from gkr_trading.core.instruments import EquityRef

            envelopes = self._feed.poll()
            for env in envelopes:
                ticker = ""
                if isinstance(env.instrument_ref, EquityRef):
                    ticker = env.instrument_ref.ticker
                else:
                    ticker = env.instrument_ref.canonical_key

                snap = MarketDataSnapshot(
                    ticker=ticker,
                    last_cents=env.last_cents,
                    bid_cents=env.bid_cents,
                    ask_cents=env.ask_cents,
                    open_cents=env.open_cents,
                    high_cents=env.high_cents,
                    low_cents=env.low_cents,
                    close_cents=env.close_cents,
                    volume=env.volume,
                    delta=env.delta,
                    theta=env.theta,
                    implied_vol=env.implied_vol,
                )
                snapshots.append(snap)

                # Track price history for sparklines
                if env.close_cents is not None:
                    hist = self._price_history.setdefault(ticker, [])
                    hist.append(env.close_cents)
                    # Keep last 120 points (30 min at 15s polls)
                    if len(hist) > 120:
                        self._price_history[ticker] = hist[-120:]

        except Exception as exc:
            logger.error(f"Market data poll error: {exc}")

        return snapshots, market_open
