"""Background market data poller — feeds live prices to the TUI.

Runs in a Textual worker thread.  Polls yfinance every 15 s
and posts MarketDataMessage to the app message queue.

Migration note: Alpaca market data API (data.alpaca.markets) required a paid
subscription.  yfinance provides free real-time-ish quotes that work with
any environment.  Alpaca broker API (paper-api.alpaca.markets) for orders,
positions, and account remains fully intact.
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
    """Stateful poller — call ``poll_once`` repeatedly from a worker loop.

    Uses yfinance for market data (free, no subscription required).
    Alpaca broker API (paper-api.alpaca.markets) is used only for
    market open/close status via /v2/clock.
    """

    def __init__(
        self,
        equity_tickers: Sequence[str] | None = None,
        poll_interval_sec: float = 15.0,
    ) -> None:
        if equity_tickers is None:
            import os
            env_list = os.environ.get("GKR_WATCHLIST", os.environ.get("ALPACA_WATCHLIST", "")).strip()
            if env_list:
                equity_tickers = tuple(t.strip().upper() for t in env_list.split(",") if t.strip())
            else:
                equity_tickers = ("AAPL", "SPY", "QQQ", "TSLA", "NVDA", "MSFT")
        self._tickers = list(equity_tickers)
        self._interval = poll_interval_sec
        self._metadata: Any = None  # AlpacaMarketMetadataProvider for clock
        self._broker_http: Any = None
        self._available = False
        self._last_market_open: Optional[bool] = None
        self._price_history: dict[str, list[int]] = {}
        self._latest_snapshots: dict[str, MarketDataSnapshot] = {}
        self._init()

    def _init(self) -> None:
        try:
            import yfinance  # noqa: F401
            self._available = True
            logger.info(
                f"MarketPoller: yfinance ready, tickers={self._tickers}"
            )
        except ImportError:
            logger.warning("MarketPoller: yfinance not installed — market data unavailable")
            self._available = False
            return

        # Optional: Alpaca broker API for market open/close status
        try:
            from gkr_trading.live.alpaca_config import AlpacaPaperConfig
            from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient
            from gkr_trading.live.market_metadata_provider import (
                AlpacaMarketMetadataProvider,
            )
            cfg = AlpacaPaperConfig.from_env()
            self._broker_http = UrllibAlpacaHttpClient(config=cfg)
            self._metadata = AlpacaMarketMetadataProvider(self._broker_http)
            logger.info(f"MarketPoller: Alpaca broker API ready for clock (base_url={cfg.base_url})")
        except Exception as exc:
            logger.info(f"MarketPoller: Alpaca broker unavailable for clock (non-fatal): {exc}")
            self._metadata = None

    @property
    def available(self) -> bool:
        return self._available

    @property
    def interval(self) -> float:
        return self._interval

    def get_price_history(self, ticker: str) -> list[int]:
        """Return collected close prices for sparkline charting."""
        return list(self._price_history.get(ticker, []))

    def get_all_snapshots(self) -> dict[str, MarketDataSnapshot]:
        """Return the most recent snapshot for each ticker as a dict."""
        return dict(self._latest_snapshots)

    def poll_once(self) -> tuple[list[MarketDataSnapshot], Optional[bool]]:
        """Execute one poll cycle.

        Returns (snapshots, market_open_or_None).
        Raises on unrecoverable errors.
        """
        if not self._available:
            return [], None

        snapshots: list[MarketDataSnapshot] = []
        market_open: Optional[bool] = None

        # Check market status via Alpaca broker API (if available)
        try:
            if self._metadata:
                market_open = self._metadata.is_market_open()
        except Exception as exc:
            logger.debug(f"MarketPoller clock check failed (non-fatal): {exc}")

        # Poll market data via yfinance
        try:
            snapshots = self._poll_yfinance()
        except Exception as exc:
            logger.error(f"Market data poll error: {exc}")

        return snapshots, market_open

    def _poll_yfinance(self) -> list[MarketDataSnapshot]:
        """Fetch current quotes from yfinance for all tickers."""
        import yfinance as yf

        snapshots: list[MarketDataSnapshot] = []
        try:
            tickers_str = " ".join(self._tickers)
            tickers_obj = yf.Tickers(tickers_str)

            for ticker_sym in self._tickers:
                try:
                    t = tickers_obj.tickers.get(ticker_sym)
                    if t is None:
                        logger.warning(f"MarketPoller: yfinance returned None for {ticker_sym}")
                        continue

                    info = t.fast_info
                    last_price = getattr(info, "last_price", None)
                    if last_price is None:
                        logger.debug(f"MarketPoller: no last_price for {ticker_sym}")
                        continue

                    open_price = getattr(info, "open", None)
                    day_high = getattr(info, "day_high", None)
                    day_low = getattr(info, "day_low", None)
                    prev_close = getattr(info, "previous_close", None)
                    last_volume = getattr(info, "last_volume", None)

                    last_cents = int(round(last_price * 100))
                    open_cents = int(round(open_price * 100)) if open_price else None
                    high_cents = int(round(day_high * 100)) if day_high else None
                    low_cents = int(round(day_low * 100)) if day_low else None
                    prev_close_cents = int(round(prev_close * 100)) if prev_close else None
                    volume = int(last_volume) if last_volume else None

                    snap = MarketDataSnapshot(
                        ticker=ticker_sym,
                        last_cents=last_cents,
                        open_cents=open_cents,
                        high_cents=high_cents,
                        low_cents=low_cents,
                        close_cents=last_cents,  # close = last for intraday
                        volume=volume,
                        prev_close_cents=prev_close_cents,
                    )
                    snapshots.append(snap)
                    self._latest_snapshots[ticker_sym] = snap

                    # Track price history for sparklines
                    hist = self._price_history.setdefault(ticker_sym, [])
                    hist.append(last_cents)
                    if len(hist) > 120:
                        self._price_history[ticker_sym] = hist[-120:]

                except Exception as exc:
                    logger.warning(f"MarketPoller: failed to fetch {ticker_sym}: {exc}")

        except Exception as exc:
            logger.error(f"MarketPoller yfinance batch error: {exc}")

        logger.info(f"MarketPoller: polled {len(snapshots)}/{len(self._tickers)} tickers via yfinance")
        return snapshots
