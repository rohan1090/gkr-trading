"""Always-on observation engine — runs data polling in independent threads
regardless of market hours.

Publishes to :class:`DataBus` and writes to :class:`MarketSnapshotCache` /
:class:`PositionCache` on every poll cycle.  This is the core of the
observation plane: data collection never stops, even when no trading
session is running.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from typing import Any, Optional

from gkr_trading.live.data_bus import (
    DataBus,
    TOPIC_ACCOUNT,
    TOPIC_MARKET_SNAPSHOT,
    TOPIC_MARKET_STATUS,
    TOPIC_OHLCV_BAR,
    TOPIC_POSITIONS,
    get_default_bus,
)
from gkr_trading.persistence.market_cache import MarketSnapshotCache
from gkr_trading.persistence.position_cache import PositionCache

logger = logging.getLogger(__name__)

_DEFAULT_EQUITY_TICKERS = [
    "SPY", "QQQ", "AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "META", "GOOGL", "AMD",
]

ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"


class ObservationPlane:
    """Always-on data observation engine.

    Starts five background threads:
      1. Clock — polls ``/v2/clock`` every 60 s
      2. Snapshots — polls ``/v2/stocks/snapshots`` every *alpaca_snapshot_interval* s
      3. Positions — polls ``/v2/positions`` every *alpaca_positions_interval* s
      4. Account — polls ``/v2/account`` every *alpaca_positions_interval* s
      5. Bars — polls ``/v2/stocks/bars`` every *alpaca_bars_interval* s (off-hours only)
    """

    def __init__(
        self,
        db_path: str,
        data_bus: Optional[DataBus] = None,
        alpaca_snapshot_interval: float = 15.0,
        alpaca_positions_interval: float = 10.0,
        alpaca_bars_interval: float = 300.0,
        schwab_enabled: bool = False,
    ) -> None:
        self._db_path = db_path
        self._bus = data_bus or get_default_bus()
        self._snapshot_interval = alpaca_snapshot_interval
        self._positions_interval = alpaca_positions_interval
        self._bars_interval = alpaca_bars_interval
        self._schwab_enabled = schwab_enabled

        # Caches — own connection with relaxed sync for cache data
        self._db_conn = self._open_cache_conn(db_path)
        self._market_cache = MarketSnapshotCache(self._db_conn)
        self._position_cache = PositionCache(self._db_conn)

        # State
        self._stop_event = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._market_open: bool = False
        self._alpaca_available: bool = False

        # Tickers
        self._equity_tickers, self._options_tickers = self._load_tickers_from_env()

        # HTTP clients (lazy-init on first use)
        self._data_http: Any = None
        self._broker_http: Any = None

    # ── Public API ─────────────────────────────────────────────────

    def start(self) -> None:
        """Start all polling threads.  Idempotent."""
        if self._threads:
            return

        self._init_alpaca_clients()

        thread_specs = [
            ("_clock_thread", self._clock_loop),
            ("_snapshot_thread", self._snapshot_loop),
            ("_positions_thread", self._positions_loop),
            ("_account_thread", self._account_loop),
            ("_bars_thread", self._bars_loop),
        ]
        for name, target in thread_specs:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads[name] = t

        logger.info(
            f"ObservationPlane started — {len(self._equity_tickers)} tickers, "
            f"snapshot={self._snapshot_interval}s, positions={self._positions_interval}s"
        )

    def stop(self) -> None:
        """Signal all threads to stop and join them."""
        self._stop_event.set()
        for name, t in self._threads.items():
            t.join(timeout=5.0)
            if t.is_alive():
                logger.warning(f"ObservationPlane thread {name} did not stop in time")
        self._threads.clear()
        logger.info("ObservationPlane stopped")

    def is_running(self) -> bool:
        return bool(self._threads) and any(t.is_alive() for t in self._threads.values())

    @property
    def status(self) -> str:
        if self.is_running():
            return "running"
        return "stopped"

    def get_tickers(self) -> list[str]:
        return list(self._equity_tickers) + list(self._options_tickers)

    def force_refresh_positions(self):
        """Blocking immediate poll of positions + account.  Returns (positions, account)."""
        positions = self._poll_positions_once()
        account_ok = self._poll_account_once()
        cached_positions = self._position_cache.get_positions(source="alpaca_paper")
        cached_account = self._position_cache.get_account(source="alpaca_paper")
        return cached_positions, cached_account

    # ── Thread loops ───────────────────────────────────────────────

    def _clock_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._broker_http:
                    raw = self._broker_http.request_json("GET", "/v2/clock")
                    is_open = raw.get("is_open", False)
                    if is_open != self._market_open:
                        self._market_open = is_open
                    self._bus.publish(TOPIC_MARKET_STATUS, {
                        "is_open": is_open,
                        "next_open": raw.get("next_open", ""),
                        "next_close": raw.get("next_close", ""),
                    })
            except Exception as exc:
                logger.debug(f"Clock poll error: {exc}")
            self._interruptible_sleep(60.0)

    def _snapshot_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll_snapshots_once()
            except Exception as exc:
                logger.error(f"Snapshot poll error: {exc}")
            self._interruptible_sleep(self._snapshot_interval)

    def _positions_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll_positions_once()
            except Exception as exc:
                logger.error(f"Positions poll error: {exc}")
            self._interruptible_sleep(self._positions_interval)

    def _account_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll_account_once()
            except Exception as exc:
                logger.error(f"Account poll error: {exc}")
            self._interruptible_sleep(self._positions_interval)

    def _bars_loop(self) -> None:
        """Only polls bars when market is closed (off-hours enrichment)."""
        while not self._stop_event.is_set():
            if not self._market_open:
                try:
                    self._poll_bars_once()
                except Exception as exc:
                    logger.error(f"Bars poll error: {exc}")
            self._interruptible_sleep(self._bars_interval)

    # ── Polling methods ────────────────────────────────────────────

    def _poll_snapshots_once(self) -> int:
        """Poll yfinance for equity snapshots.  Returns count of tickers updated.

        Falls back to Alpaca /v2/stocks/snapshots only if yfinance is unavailable.
        """
        if not self._equity_tickers:
            return 0

        # Primary: yfinance (free, no subscription)
        try:
            return self._poll_snapshots_yfinance()
        except Exception as exc:
            logger.warning(f"yfinance snapshot poll failed, trying Alpaca fallback: {exc}")

        # Fallback: Alpaca data API (requires paid subscription)
        if not self._data_http:
            return 0

        try:
            symbols = ",".join(self._equity_tickers)
            raw = self._data_http.request_json(
                "GET", "/v2/stocks/snapshots", query={"symbols": symbols, "feed": "iex"}
            )
            if not isinstance(raw, dict):
                return 0

            count = 0
            for ticker, snap_data in raw.items():
                parsed = self._parse_single_snapshot(ticker, snap_data)
                if parsed:
                    self._market_cache.upsert_snapshot(ticker, parsed)
                    self._bus.publish(TOPIC_MARKET_SNAPSHOT, parsed)
                    count += 1
            return count
        except Exception as exc:
            logger.error(f"Alpaca snapshot fallback also failed: {exc}")
            return 0

    def _poll_snapshots_yfinance(self) -> int:
        """Poll yfinance for equity snapshots.  Returns count of tickers updated."""
        import yfinance as yf

        tickers_str = " ".join(self._equity_tickers)
        tickers_obj = yf.Tickers(tickers_str)

        count = 0
        for ticker_sym in self._equity_tickers:
            try:
                t = tickers_obj.tickers.get(ticker_sym)
                if t is None:
                    continue

                info = t.fast_info
                last_price = getattr(info, "last_price", None)
                if last_price is None:
                    continue

                open_price = getattr(info, "open", None)
                day_high = getattr(info, "day_high", None)
                day_low = getattr(info, "day_low", None)
                prev_close = getattr(info, "previous_close", None)
                last_volume = getattr(info, "last_volume", None)

                parsed = {
                    "ticker": ticker_sym,
                    "last_cents": int(round(last_price * 100)),
                    "open_cents": int(round(open_price * 100)) if open_price else None,
                    "high_cents": int(round(day_high * 100)) if day_high else None,
                    "low_cents": int(round(day_low * 100)) if day_low else None,
                    "prev_close_cents": int(round(prev_close * 100)) if prev_close else None,
                    "volume": int(last_volume) if last_volume else None,
                    "source": "yfinance",
                }
                self._market_cache.upsert_snapshot(ticker_sym, parsed)
                self._bus.publish(TOPIC_MARKET_SNAPSHOT, parsed)
                count += 1
            except Exception as exc:
                logger.warning(f"yfinance snapshot for {ticker_sym}: {exc}")

        return count

    def _poll_positions_once(self) -> int:
        """Poll Alpaca /v2/positions.  Returns count of positions updated."""
        if not self._broker_http:
            return 0

        raw = self._broker_http.request_json("GET", "/v2/positions")
        if not isinstance(raw, list):
            return 0

        positions = []
        for item in raw:
            try:
                positions.append({
                    "ticker": item.get("symbol", ""),
                    "qty": float(item.get("qty", 0)),
                    "side": item.get("side", "long"),
                    "avg_entry_cents": int(float(item.get("avg_entry_price", 0)) * 100),
                    "last_cents": int(float(item.get("current_price", 0)) * 100),
                    "unrealized_pnl_cents": int(float(item.get("unrealized_pl", 0)) * 100),
                    "unrealized_pnl_pct": float(item.get("unrealized_plpc", 0)) * 100,
                    "market_value_cents": int(float(item.get("market_value", 0)) * 100),
                    "cost_basis_cents": int(float(item.get("cost_basis", 0)) * 100),
                    "asset_class": item.get("asset_class", "us_equity"),
                })
            except Exception as exc:
                logger.warning(f"Failed to parse position: {exc}")

        self._position_cache.upsert_positions(positions, source="alpaca_paper")
        self._bus.publish(TOPIC_POSITIONS, {
            "source": "alpaca_paper", "positions": positions,
        })
        return len(positions)

    def _poll_account_once(self) -> bool:
        """Poll Alpaca /v2/account.  Returns True on success."""
        if not self._broker_http:
            return False

        raw = self._broker_http.request_json("GET", "/v2/account")
        if not isinstance(raw, dict):
            return False

        account = {
            "cash_cents": int(float(raw.get("cash", 0)) * 100),
            "portfolio_value_cents": int(float(raw.get("portfolio_value", 0)) * 100),
            "buying_power_cents": int(float(raw.get("buying_power", 0)) * 100),
            "equity_cents": int(float(raw.get("equity", 0)) * 100),
            "unrealized_pnl_cents": int(float(raw.get("unrealized_pl", 0)) * 100),
        }
        self._position_cache.upsert_account(account, source="alpaca_paper")
        self._bus.publish(TOPIC_ACCOUNT, {"source": "alpaca_paper", "account": account})
        return True

    def _poll_bars_once(self) -> int:
        """Poll for OHLCV bars data.  Uses yfinance primary, Alpaca fallback.
        Returns ticker count."""
        if not self._equity_tickers:
            return 0

        # Primary: yfinance
        try:
            return self._poll_bars_yfinance()
        except Exception as exc:
            logger.warning(f"yfinance bars poll failed, trying Alpaca fallback: {exc}")

        # Fallback: Alpaca data API
        if not self._data_http:
            return 0

        try:
            symbols = ",".join(self._equity_tickers)
            raw = self._data_http.request_json(
                "GET", "/v2/stocks/bars",
                query={
                    "symbols": symbols,
                    "timeframe": "1Day",
                    "limit": "1",
                    "sort": "desc",
                    "adjustment": "raw",
                },
            )
            if not isinstance(raw, dict):
                return 0

            bars_data = raw.get("bars", {})
            count = 0
            for ticker, bars in bars_data.items():
                if not bars:
                    continue
                bar = bars[0]
                data = {
                    "ticker": ticker,
                    "last_cents": int(float(bar.get("c", 0)) * 100),
                    "open_cents": int(float(bar.get("o", 0)) * 100),
                    "high_cents": int(float(bar.get("h", 0)) * 100),
                    "low_cents": int(float(bar.get("l", 0)) * 100),
                    "volume": bar.get("v"),
                    "timestamp_utc": bar.get("t", ""),
                    "source": "alpaca_bars",
                }
                self._market_cache.upsert_snapshot(ticker, data)
                self._bus.publish(TOPIC_OHLCV_BAR, {
                    "ticker": ticker, "timeframe": "1Day", "bars": bars,
                })
                count += 1
            return count
        except Exception as exc:
            logger.error(f"Alpaca bars fallback also failed: {exc}")
            return 0

    def _poll_bars_yfinance(self) -> int:
        """Poll yfinance for daily OHLCV bars."""
        import yfinance as yf

        tickers_str = " ".join(self._equity_tickers)
        tickers_obj = yf.Tickers(tickers_str)

        count = 0
        for ticker_sym in self._equity_tickers:
            try:
                t = tickers_obj.tickers.get(ticker_sym)
                if t is None:
                    continue

                info = t.fast_info
                last_price = getattr(info, "last_price", None)
                if last_price is None:
                    continue

                open_price = getattr(info, "open", None)
                day_high = getattr(info, "day_high", None)
                day_low = getattr(info, "day_low", None)
                last_volume = getattr(info, "last_volume", None)

                data = {
                    "ticker": ticker_sym,
                    "last_cents": int(round(last_price * 100)),
                    "open_cents": int(round(open_price * 100)) if open_price else None,
                    "high_cents": int(round(day_high * 100)) if day_high else None,
                    "low_cents": int(round(day_low * 100)) if day_low else None,
                    "volume": int(last_volume) if last_volume else None,
                    "source": "yfinance_bars",
                }
                self._market_cache.upsert_snapshot(ticker_sym, data)
                self._bus.publish(TOPIC_OHLCV_BAR, {
                    "ticker": ticker_sym, "timeframe": "1Day",
                    "bars": [data],
                })
                count += 1
            except Exception as exc:
                logger.warning(f"yfinance bars for {ticker_sym}: {exc}")

        return count

    # ── Helpers ────────────────────────────────────────────────────

    def _parse_single_snapshot(self, ticker: str, snap: dict) -> Optional[dict]:
        """Parse a single Alpaca snapshot response into cache dict."""
        latest_trade = snap.get("latestTrade", {})
        latest_quote = snap.get("latestQuote", {})
        minute_bar = snap.get("minuteBar", {})
        daily_bar = snap.get("dailyBar", {})
        prev_bar = snap.get("prevDailyBar", {})

        last_price = latest_trade.get("p")
        if last_price is None:
            return None

        bar = minute_bar or daily_bar

        return {
            "ticker": ticker,
            "last_cents": int(float(last_price) * 100),
            "open_cents": int(float(bar.get("o", 0)) * 100) if bar.get("o") else None,
            "high_cents": int(float(bar.get("h", 0)) * 100) if bar.get("h") else None,
            "low_cents": int(float(bar.get("l", 0)) * 100) if bar.get("l") else None,
            "prev_close_cents": int(float(prev_bar.get("c", 0)) * 100) if prev_bar.get("c") else None,
            "volume": bar.get("v"),
            "vwap_cents": int(float(bar.get("vw", 0)) * 100) if bar.get("vw") else None,
            "timestamp_utc": latest_trade.get("t", ""),
            "source": "alpaca_snapshot",
        }

    def _init_alpaca_clients(self) -> None:
        """Create HTTP clients for data + broker APIs."""
        try:
            from gkr_trading.live.alpaca_config import AlpacaPaperConfig
            from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient

            cfg = AlpacaPaperConfig.from_env()

            # Data client → data.alpaca.markets
            data_cfg = AlpacaPaperConfig(
                api_key=cfg.api_key,
                secret_key=cfg.secret_key,
                base_url=ALPACA_DATA_BASE_URL,
            )
            self._data_http = UrllibAlpacaHttpClient(config=data_cfg)

            # Broker client → paper-api.alpaca.markets
            self._broker_http = UrllibAlpacaHttpClient(config=cfg)

            self._alpaca_available = True
            logger.info(
                f"ObservationPlane Alpaca clients ready: "
                f"data={ALPACA_DATA_BASE_URL}, broker={cfg.base_url}"
            )
        except Exception as exc:
            logger.warning(f"Alpaca clients unavailable: {exc}", exc_info=True)
            self._alpaca_available = False

    @staticmethod
    def _load_tickers_from_env() -> tuple[list[str], list[str]]:
        """Load ticker lists from environment, fall back to defaults."""
        eq_raw = os.environ.get("ALPACA_EQUITY_TICKERS", "").strip()
        if eq_raw:
            equity = [t.strip().upper() for t in eq_raw.split(",") if t.strip()]
        else:
            equity = list(_DEFAULT_EQUITY_TICKERS)

        opt_raw = os.environ.get("ALPACA_OPTIONS_TICKERS", "").strip()
        options = [t.strip().upper() for t in opt_raw.split(",") if t.strip()] if opt_raw else []

        return equity, options

    @staticmethod
    def _open_cache_conn(db_path: str) -> sqlite3.Connection:
        """Open a SQLite connection for cache tables (WAL, NORMAL sync)."""
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-64000")
        return conn

    def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep in 1-second chunks so stop_event can interrupt quickly."""
        elapsed = 0.0
        while elapsed < seconds and not self._stop_event.is_set():
            time.sleep(min(1.0, seconds - elapsed))
            elapsed += 1.0
