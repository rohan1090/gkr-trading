\"\"\"Market data feed — real Alpaca market data polling for the V2 runtime.

Fetches live bars/snapshots from Alpaca's market data API and constructs
MarketDataEnvelope objects for strategy consumption.

Two modes:
- Equity: polls /v2/stocks/snapshots for configured tickers
- Options: polls /v1beta1/options/snapshots for configured OCC symbols

Stale-data detection: skips envelopes whose timestamp hasn't changed since
the last poll cycle.  Market-data errors are counted; the feed signals
\"too many consecutive failures\" so the runtime can halt.
\"\"\"
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from gkr_trading.core.instruments import EquityRef, InstrumentRef, OptionsRef
from gkr_trading.core.market_data import MarketDataEnvelope
from gkr_trading.live.traditional.options.options_domain import OCCSymbolParser

logger = logging.getLogger(__name__)


@dataclass
class MarketDataFeedConfig:
    \"\"\"Configuration for the market data feed.\"\"\"
    equity_tickers: Sequence[str] = ()
    options_occ_symbols: Sequence[str] = ()
    poll_interval_sec: float = 15.0
    max_consecutive_failures: int = 5
    stale_threshold_sec: float = 120.0  # data older than this is stale
    drop_stale: bool = True  # False for TUI (always show latest data even if unchanged)


@dataclass
class FeedStats:
    polls: int = 0
    envelopes_produced: int = 0
    stale_skips: int = 0
    errors: int = 0
    consecutive_failures: int = 0


class AlpacaMarketDataFeed:
    \"\"\"Polls Alpaca market data and produces MarketDataEnvelope objects.

    NOT a streaming feed — polls at configured intervals.
    Designed to be called from the runtime's session loop.
    \"\"\"

    def __init__(
        self,
        http_client: Any,
        config: MarketDataFeedConfig,
    ) -> None:
        self._http = http_client
        self._config = config
        self._stats = FeedStats()
        self._last_timestamps: Dict[str, int] = {}

    @property
    def stats(self) -> FeedStats:
        return self._stats

    @property
    def has_fatal_failure(self) -> bool:
        return self._stats.consecutive_failures >= self._config.max_consecutive_failures

    def poll(self) -> List[MarketDataEnvelope]:
        \"\"\"Poll Alpaca for current market data. Returns list of envelopes.

        Each envelope corresponds to one instrument.  Stale data (timestamp
        unchanged since last poll) is filtered out.
        \"\"\"
        self._stats.polls += 1
        envelopes: List[MarketDataEnvelope] = []

        # Poll equities — snapshot first, bars as fallback
        if self._config.equity_tickers:
            try:
                eq_envelopes = self._poll_equity_snapshots()
                if not eq_envelopes or all(
                    getattr(e, 'last_cents', None) is None for e in eq_envelopes
                ):
                    logger.info(\"Snapshot data empty — falling back to historical bars\")
                    eq_envelopes = self._poll_historical_bars(timeframe=\"1Day\", limit=1)
                envelopes.extend(eq_envelopes)
                self._stats.consecutive_failures = 0
            except Exception as exc:
                self._stats.errors += 1
                self._stats.consecutive_failures += 1
                logger.error(f\"Equity snapshot poll failed: {exc}\")

        # Poll options
        if self._config.options_occ_symbols:
            try:
                opt_envelopes = self._poll_options_snapshots()
                envelopes.extend(opt_envelopes)
                self._stats.consecutive_failures = 0
            except Exception as exc:
                self._stats.errors += 1
                self._stats.consecutive_failures += 1
                logger.error(f\"Options snapshot poll failed: {exc}\")

        self._stats.envelopes_produced += len(envelopes)
        return envelopes

    def _poll_equity_snapshots(self) -> List[MarketDataEnvelope]:
        \"\"\"Fetch equity snapshots from Alpaca /v2/stocks/snapshots.\"\"\"
        tickers = list(self._config.equity_tickers)
        if not tickers:
            return []

        symbols_param = \",\".join(t.upper() for t in tickers)
        data = self._http.request_json(
            \"GET\",
            \"/v2/stocks/snapshots\",
            query={\"symbols\": symbols_param},
        )
        if not isinstance(data, dict):
            return []

        results: List[MarketDataEnvelope] = []
        now_ns = time.time_ns()

        for ticker, snap in data.items():
            try:
                env = self._equity_snapshot_to_envelope(ticker, snap, now_ns)
                if env is not None:
                    results.append(env)
            except Exception as exc:
                logger.warning(f\"Failed to parse equity snapshot for {ticker}: {exc}\")
        return results

    def _equity_snapshot_to_envelope(
        self, ticker: str, snap: dict, now_ns: int,
    ) -> Optional[MarketDataEnvelope]:
        \"\"\"Convert a single Alpaca equity snapshot to MarketDataEnvelope.\"\"\"
        latest_trade = snap.get(\"latestTrade\", {})
        latest_quote = snap.get(\"latestQuote\", {})
        daily_bar = snap.get(\"dailyBar\", {})
        minute_bar = snap.get(\"minuteBar\", {})

        # Prefer minute bar for recent data, daily bar for OHLC
        bar = minute_bar if minute_bar else daily_bar

        # Stale detection: use trade timestamp
        trade_ts = latest_trade.get(\"t\", \"\")
        ts_key = f\"equity:{ticker}\"
        ts_ns = _parse_rfc3339_ns(trade_ts) if trade_ts else now_ns

        if ts_key in self._last_timestamps and self._last_timestamps[ts_key] == ts_ns:
            self._stats.stale_skips += 1
            # For TUI display, pass through stale data instead of dropping.
            # Strategy engine callers default to drop_stale=True.
            if getattr(self._config, 'drop_stale', True):
                return None
        self._last_timestamps[ts_key] = ts_ns

        last_cents = _dollars_to_cents(latest_trade.get(\"p\"))
        bid_cents = _dollars_to_cents(latest_quote.get(\"bp\"))
        ask_cents = _dollars_to_cents(latest_quote.get(\"ap\"))
        open_cents = _dollars_to_cents(bar.get(\"o\"))
        high_cents = _dollars_to_cents(bar.get(\"h\"))
        low_cents = _dollars_to_cents(bar.get(\"l\"))
        close_cents = _dollars_to_cents(bar.get(\"c\"))
        volume = int(bar.get(\"v\", 0)) if bar.get(\"v\") else None

        return MarketDataEnvelope(
            instrument_ref=EquityRef(ticker=ticker.upper()),
            timestamp_ns=ts_ns,
            open_cents=open_cents,
            high_cents=high_cents,
            low_cents=low_cents,
            close_cents=close_cents,
            volume=volume,
            bid_cents=bid_cents,
            ask_cents=ask_cents,
            last_cents=last_cents,
        )

    def _poll_options_snapshots(self) -> List[MarketDataEnvelope]:
        \"\"\"Fetch options snapshots from Alpaca /v1beta1/options/snapshots.\"\"\"
        occ_symbols = list(self._config.options_occ_symbols)
        if not occ_symbols:
            return []

        symbols_param = \",\".join(occ_symbols)
        data = self._http.request_json(
            \"GET\",
            \"/v1beta1/options/snapshots\",
            query={\"symbols\": symbols_param, \"feed\": \"indicative\"},
        )
        if not isinstance(data, dict):
            return []

        # Alpaca returns { \"snapshots\": { \"OCC_SYMBOL\": {...}, ... } }
        snapshots = data.get(\"snapshots\", data)
        if not isinstance(snapshots, dict):
            return []

        results: List[MarketDataEnvelope] = []
        now_ns = time.time_ns()

        for occ_symbol, snap in snapshots.items():
            try:
                env = self._options_snapshot_to_envelope(occ_symbol, snap, now_ns)
                if env is not None:
                    results.append(env)
            except Exception as exc:
                logger.warning(f\"Failed to parse options snapshot for {occ_symbol}: {exc}\")
        return results

    def _options_snapshot_to_envelope(
        self, occ_symbol: str, snap: dict, now_ns: int,
    ) -> Optional[MarketDataEnvelope]:
        \"\"\"Convert a single Alpaca options snapshot to MarketDataEnvelope.\"\"\"
        latest_trade = snap.get(\"latestTrade\", {})
        latest_quote = snap.get(\"latestQuote\", {})
        greeks = snap.get(\"greeks\", {})

        # Stale detection
        trade_ts = latest_trade.get(\"t\", \"\")
        ts_key = f\"option:{occ_symbol}\"
        ts_ns = _parse_rfc3339_ns(trade_ts) if trade_ts else now_ns

        if ts_key in self._last_timestamps and self._last_timestamps[ts_key] == ts_ns:
            self._stats.stale_skips += 1
            return None
        self._last_timestamps[ts_key] = ts_ns

        try:
            ref = OCCSymbolParser.parse(occ_symbol)
        except Exception:
            logger.warning(f\"Could not parse OCC symbol: {occ_symbol}\")
            return None

        last_cents = _dollars_to_cents(latest_trade.get(\"p\"))
        bid_cents = _dollars_to_cents(latest_quote.get(\"bp\"))
        ask_cents = _dollars_to_cents(latest_quote.get(\"ap\"))

        return MarketDataEnvelope(
            instrument_ref=ref,
            timestamp_ns=ts_ns,
            last_cents=last_cents,
            bid_cents=bid_cents,
            ask_cents=ask_cents,
            implied_vol=greeks.get(\"implied_volatility\"),
            delta=greeks.get(\"delta\"),
            gamma=greeks.get(\"gamma\"),
            theta=greeks.get(\"theta\"),
            vega=greeks.get(\"vega\"),
            open_interest=snap.get(\"openInterest\"),
        )

    # ---- Historical bars fallback ----------------------------------------

    def _poll_historical_bars(
        self, timeframe: str = \"1Day\", limit: int = 1
    ) -> List[MarketDataEnvelope]:
        \"\"\"Fetch most recent OHLCV bar per ticker from /v2/stocks/bars.

        Works 24/7 — used as a fallback when the snapshot endpoint
        returns empty data (e.g. outside market hours).

        Sets last_cents = close_cents so the TUI market table always
        renders a price regardless of market hours.
        \"\"\"
        tickers = list(self._config.equity_tickers)
        if not tickers:
            return []

        symbols = \",\".join(tickers)
        now_ns = time.time_ns()
        raw = self._http.request_json(
            \"GET\",
            \"/v2/stocks/bars\",
            query={
                \"symbols\": symbols,
                \"timeframe\": timeframe,
                \"limit\": str(limit),
                \"sort\": \"desc\",
                \"adjustment\": \"raw\",
            },
        )

        bars_data = raw.get(\"bars\", {}) if isinstance(raw, dict) else {}
        envelopes: List[MarketDataEnvelope] = []
        for ticker, bars in bars_data.items():
            if not bars:
                continue
            bar = bars[0]
            close_cents = _dollars_to_cents(bar.get(\"c\"))
            env = MarketDataEnvelope(
                instrument_ref=EquityRef(ticker=ticker.upper()),
                timestamp_ns=now_ns,
                # last_cents mirrors close_cents so TUI always shows a price
                last_cents=close_cents,
                open_cents=_dollars_to_cents(bar.get(\"o\")),
                high_cents=_dollars_to_cents(bar.get(\"h\")),
                low_cents=_dollars_to_cents(bar.get(\"l\")),
                close_cents=close_cents,
                volume=bar.get(\"v\"),
            )
            envelopes.append(env)

        logger.info(f\"Historical bars fallback: {len(envelopes)} tickers updated\")
        return envelopes


def _dollars_to_cents(val: Any) -> Optional[int]:
    \"\"\"Convert a dollar-denominated value to cents. Returns None if missing.\"\"\"
    if val is None:
        return None
    return int(round(float(val) * 100))


def _parse_rfc3339_ns(s: str) -> int:
    \"\"\"Parse RFC3339 timestamp to nanoseconds since epoch.\"\"\"
    if not s:
        return time.time_ns()
    try:
        # Alpaca returns RFC3339: \"2024-01-15T14:30:00.123Z\"
        if s.endswith(\"Z\"):
            s = s[:-1] + \"+00:00\"
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return time.time_ns()
