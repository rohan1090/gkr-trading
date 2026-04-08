"""Market calendar provider — NYSE/NASDAQ hours check."""
from __future__ import annotations

from datetime import datetime, time, timezone


class MarketCalendarProvider:
    """Check if the market is open for trading."""

    # NYSE regular session hours (Eastern Time)
    MARKET_OPEN = time(9, 30)
    MARKET_CLOSE = time(16, 0)
    # Pre-market
    PRE_MARKET_OPEN = time(4, 0)
    # After-hours
    AFTER_HOURS_CLOSE = time(20, 0)

    def is_market_open(self, utc_now: datetime) -> bool:
        """Check if within regular trading hours (simplified, no holidays)."""
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
        et_now = utc_now.astimezone(eastern)
        t = et_now.time()
        # Mon-Fri only
        if et_now.weekday() >= 5:
            return False
        return self.MARKET_OPEN <= t < self.MARKET_CLOSE

    def is_extended_hours(self, utc_now: datetime) -> bool:
        """Check if within extended trading hours."""
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
        et_now = utc_now.astimezone(eastern)
        t = et_now.time()
        if et_now.weekday() >= 5:
            return False
        return self.PRE_MARKET_OPEN <= t < self.AFTER_HOURS_CLOSE

    def minutes_to_close(self, utc_now: datetime) -> int:
        """Minutes until market close. Negative if already closed."""
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
        et_now = utc_now.astimezone(eastern)
        close_dt = et_now.replace(
            hour=self.MARKET_CLOSE.hour, minute=self.MARKET_CLOSE.minute,
            second=0, microsecond=0
        )
        delta = (close_dt - et_now).total_seconds() / 60
        return int(delta)
