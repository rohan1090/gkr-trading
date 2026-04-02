from __future__ import annotations

from typing import Any

from gkr_trading.core.schemas.ids import InstrumentId
from gkr_trading.data.derived_views.protocols import (
    ContinuousFuturesSeriesReader,
    GreeksSnapshotReader,
    OptionChainSnapshotReader,
)


class NullDerivedViews(
    ContinuousFuturesSeriesReader, OptionChainSnapshotReader, GreeksSnapshotReader
):
    def series_instrument_at(self, series_id: str, bar_ts_utc: str) -> InstrumentId | None:
        return None

    def snapshot(self, underlying_instrument_id: InstrumentId, ts_utc: str) -> dict[str, Any] | None:
        return None

    def greeks(self, option_instrument_id: InstrumentId, ts_utc: str) -> dict[str, Any] | None:
        return None
