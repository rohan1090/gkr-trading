from __future__ import annotations

from typing import Any, Protocol

from gkr_trading.core.schemas.ids import InstrumentId


class ContinuousFuturesSeriesReader(Protocol):
    """Maps a synthetic continuous series_id to native contract bars (scaffolding)."""

    def series_instrument_at(self, series_id: str, bar_ts_utc: str) -> InstrumentId | None: ...


class OptionChainSnapshotReader(Protocol):
    def snapshot(self, underlying_instrument_id: InstrumentId, ts_utc: str) -> dict[str, Any] | None: ...


class GreeksSnapshotReader(Protocol):
    def greeks(self, option_instrument_id: InstrumentId, ts_utc: str) -> dict[str, Any] | None: ...
