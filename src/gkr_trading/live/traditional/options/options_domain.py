"""Options domain utilities — OCC symbol parser, chain helper."""
from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional, Tuple

from gkr_trading.core.instruments import OptionsRef


class OCCSymbolParser:
    """Parse and generate OCC option symbols.

    OCC format: UNDERLYING + YYMMDD + C/P + STRIKE*1000 (8 digits)
    Example: AAPL251219C00200000
    """

    @staticmethod
    def parse(occ_symbol: str) -> OptionsRef:
        """Parse an OCC symbol into an OptionsRef."""
        # Find where the date portion starts (first digit after ticker)
        i = 0
        while i < len(occ_symbol) and not occ_symbol[i].isdigit():
            i += 1

        underlying = occ_symbol[:i]
        date_str = occ_symbol[i:i+6]
        right_char = occ_symbol[i+6]
        strike_str = occ_symbol[i+7:]

        expiry = datetime.strptime(date_str, "%y%m%d").date()
        right: Literal["call", "put"] = "call" if right_char == "C" else "put"
        strike_cents = int(strike_str)  # OCC uses strike * 1000, which is strike_cents * 10
        # OCC stores strike * 1000 in 8 digits (e.g., 00200000 = $200 = 20000 cents)
        # So: occ_value / 10 = strike_cents
        strike_cents = int(strike_str) // 10

        return OptionsRef(
            underlying=underlying,
            expiry=expiry,
            strike_cents=strike_cents,
            right=right,
            style="american",  # US equity options are American style
            multiplier=100,
            deliverable=underlying,
            occ_symbol=occ_symbol,
        )

    @staticmethod
    def generate(
        underlying: str,
        expiry: date,
        right: Literal["call", "put"],
        strike_cents: int,
    ) -> str:
        """Generate an OCC symbol from structured fields."""
        date_str = expiry.strftime("%y%m%d")
        right_char = "C" if right == "call" else "P"
        # OCC stores strike * 1000 in 8 digits
        occ_strike = strike_cents * 10
        strike_str = f"{occ_strike:08d}"
        return f"{underlying}{date_str}{right_char}{strike_str}"


class OptionsChainHelper:
    """Utilities for options chain operations."""

    @staticmethod
    def is_expiring_today(ref: OptionsRef, today: date) -> bool:
        """Check if an option expires today."""
        return ref.expiry == today

    @staticmethod
    def days_to_expiry(ref: OptionsRef, today: date) -> int:
        """Days until expiration. 0 = expires today. Negative = expired."""
        return (ref.expiry - today).days

    @staticmethod
    def is_in_expiry_window(ref: OptionsRef, today: date, window_days: int = 0) -> bool:
        """Check if option is within the expiry danger window."""
        dte = (ref.expiry - today).days
        return dte <= window_days
