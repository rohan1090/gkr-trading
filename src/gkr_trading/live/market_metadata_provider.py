"""Market metadata provider — tradeability checks, market hours, expiry windows.

AlpacaMarketMetadataProvider uses the Alpaca clock endpoint for market hours
and computes real-time expiry window halts for options.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional

from gkr_trading.core.instruments import InstrumentRef, OptionsRef

logger = logging.getLogger(__name__)


class MarketMetadataProvider(ABC):
    """Abstract market metadata interface.

    Implementations provide market hours, tradeability, and options-specific
    timing constraints. Generic — no venue references.
    """

    @abstractmethod
    def is_market_open(self) -> bool:
        """Check if the market is currently open for trading."""
        ...

    @abstractmethod
    def next_market_open(self) -> Optional[datetime]:
        """Next market open time (UTC), or None if unknown."""
        ...

    @abstractmethod
    def next_market_close(self) -> Optional[datetime]:
        """Next market close time (UTC), or None if unknown."""
        ...

    @abstractmethod
    def minutes_until_close(self) -> Optional[int]:
        """Minutes until market close, or None if market is closed."""
        ...

    @abstractmethod
    def is_tradeable(self, ref: InstrumentRef) -> bool:
        """Check if an instrument is currently tradeable."""
        ...

    @abstractmethod
    def is_in_expiry_window(self, ref: OptionsRef, window_minutes: int = 60) -> bool:
        """Check if an option is within the expiry danger window.

        Window is measured in minutes before market close on expiry day.
        """
        ...


class AlpacaMarketMetadataProvider(MarketMetadataProvider):
    """Alpaca-specific market metadata using the /v2/clock endpoint.

    Caches clock data with a configurable TTL to avoid excessive API calls.
    """

    def __init__(
        self,
        http_client: Any,
        *,
        clock_cache_ttl_sec: float = 60.0,
    ) -> None:
        self._http = http_client
        self._cache_ttl = clock_cache_ttl_sec
        self._cached_clock: Optional[dict] = None
        self._cache_time: float = 0

    def _fetch_clock(self) -> dict:
        """Fetch market clock, with caching."""
        import time as time_mod
        now = time_mod.time()
        if self._cached_clock and (now - self._cache_time) < self._cache_ttl:
            return self._cached_clock
        try:
            clock = self._http.request_json("GET", "/v2/clock")
            self._cached_clock = clock
            self._cache_time = now
            return clock
        except Exception as exc:
            logger.error(f"Failed to fetch Alpaca clock: {exc}")
            # Return stale cache or empty
            if self._cached_clock:
                return self._cached_clock
            return {"is_open": False}

    def is_market_open(self) -> bool:
        clock = self._fetch_clock()
        return clock.get("is_open", False)

    def next_market_open(self) -> Optional[datetime]:
        clock = self._fetch_clock()
        raw = clock.get("next_open")
        if raw:
            return _parse_iso(raw)
        return None

    def next_market_close(self) -> Optional[datetime]:
        clock = self._fetch_clock()
        raw = clock.get("next_close")
        if raw:
            return _parse_iso(raw)
        return None

    def minutes_until_close(self) -> Optional[int]:
        close = self.next_market_close()
        if close is None:
            return None
        if not self.is_market_open():
            return None
        now = datetime.now(timezone.utc)
        delta = close - now
        return max(0, int(delta.total_seconds() / 60))

    def is_tradeable(self, ref: InstrumentRef) -> bool:
        """An instrument is tradeable if the market is open.

        For options near expiry, additional checks apply.
        """
        if not self.is_market_open():
            return False
        if isinstance(ref, OptionsRef):
            # Options on expiry day may have restricted trading near close
            today = datetime.now(timezone.utc).date()
            if ref.expiry == today:
                mins = self.minutes_until_close()
                if mins is not None and mins < 15:
                    logger.warning(
                        f"Options {ref.occ_symbol} near-expiry restricted: {mins}min to close"
                    )
                    return False
        return True

    def is_in_expiry_window(self, ref: OptionsRef, window_minutes: int = 60) -> bool:
        """Check if option is within the expiry danger window.

        True if: it's expiry day AND market closes within window_minutes.
        """
        today = datetime.now(timezone.utc).date()
        if ref.expiry != today:
            return False
        mins = self.minutes_until_close()
        if mins is None:
            return False
        return mins <= window_minutes


class ExpiryWindowHalt:
    """Real-time expiry window halt using MarketMetadataProvider.

    Blocks new opening orders on options that expire within the configured
    window before market close.
    """

    def __init__(
        self,
        metadata_provider: MarketMetadataProvider,
        window_minutes: int = 60,
    ) -> None:
        self._metadata = metadata_provider
        self._window_minutes = window_minutes

    def is_blocked(self, ref: InstrumentRef) -> bool:
        """Check if an instrument is blocked by the expiry window."""
        if not isinstance(ref, OptionsRef):
            return False
        return self._metadata.is_in_expiry_window(ref, self._window_minutes)

    def check_intent(self, intent: Any) -> Optional[str]:
        """Check if a trade intent is blocked by expiry window.

        Returns block reason string, or None if allowed.
        Closing orders are always allowed (sell_to_close, buy_to_close).
        """
        from gkr_trading.core.options_intents import TradeIntent as NewTradeIntent
        if not isinstance(intent, NewTradeIntent):
            return None

        ref = intent.instrument_ref
        if not isinstance(ref, OptionsRef):
            return None

        # Closing orders are always allowed even in expiry window
        if intent.action in ("sell_to_close", "buy_to_close"):
            return None

        if self.is_blocked(ref):
            mins = self._metadata.minutes_until_close()
            return (
                f"ExpiryWindowHalt: {ref.occ_symbol} expires today, "
                f"{mins}min to close (window={self._window_minutes}min)"
            )

        return None


def _parse_iso(s: str) -> datetime:
    """Parse ISO datetime string to timezone-aware datetime."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)
