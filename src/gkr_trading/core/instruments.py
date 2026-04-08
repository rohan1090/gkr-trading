"""Canonical instrument model — InstrumentRef hierarchy.

Strategy uses InstrumentRef; never raw OCC strings or ticker strings.
All types are frozen dataclasses for replay safety.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal


@dataclass(frozen=True)
class InstrumentRef:
    """Abstract base for all tradeable instruments."""
    asset_class: str  # "equity" or "option" — narrowed by subclasses

    @property
    def canonical_key(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class EquityRef(InstrumentRef):
    """Equity instrument reference."""
    ticker: str = ""
    asset_class: str = field(default="equity", init=False)

    def __post_init__(self) -> None:
        if not self.ticker or not self.ticker.strip():
            raise ValueError("ticker must be non-empty")

    @property
    def canonical_key(self) -> str:
        return f"equity:{self.ticker}"


@dataclass(frozen=True)
class OptionsRef(InstrumentRef):
    """Canonical options instrument.

    All structured fields are the source of truth.
    occ_symbol is derived/stored for audit but NOT the primary key.
    strike_cents avoids float rounding errors in position accounting.
    """
    underlying: str = ""
    expiry: date = field(default_factory=lambda: date(1970, 1, 1))
    strike_cents: int = 0      # 20000 = $200.00 strike
    right: str = "call"        # "call" or "put"
    style: str = "american"    # "american" or "european"
    multiplier: int = 100      # 100 for standard US equity options
    deliverable: str = ""      # "AAPL" for standard; may differ for adjusted
    occ_symbol: str = ""       # "AAPL251219C00200000" — derived, stored for audit
    asset_class: str = field(default="option", init=False)

    def __post_init__(self) -> None:
        if not self.underlying or not self.underlying.strip():
            raise ValueError("underlying must be non-empty")
        if self.strike_cents <= 0:
            raise ValueError("strike_cents must be positive")
        if self.multiplier <= 0:
            raise ValueError("multiplier must be positive")
        if not self.occ_symbol:
            raise ValueError("occ_symbol must be non-empty")

    @property
    def canonical_key(self) -> str:
        return f"option:{self.occ_symbol}"

    @property
    def strike_dollars(self) -> float:
        return self.strike_cents / 100.0
