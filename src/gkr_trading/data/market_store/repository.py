from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from decimal import Decimal

from gkr_trading.core.schemas.enums import AssetClass, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId


@dataclass(frozen=True)
class BarRow:
    instrument_id: InstrumentId
    timeframe: str
    bar_ts_utc: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


class MarketDataRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def insert_equity_bar(self, bar: BarRow) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO equity_bars
            (instrument_id, timeframe, bar_ts_utc, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                str(bar.instrument_id),
                bar.timeframe,
                bar.bar_ts_utc,
                str(bar.open),
                str(bar.high),
                str(bar.low),
                str(bar.close),
                str(bar.volume),
            ),
        )
        self._conn.commit()

    def insert_futures_bar(self, bar: BarRow) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO futures_bars
            (instrument_id, timeframe, bar_ts_utc, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                str(bar.instrument_id),
                bar.timeframe,
                bar.bar_ts_utc,
                str(bar.open),
                str(bar.high),
                str(bar.low),
                str(bar.close),
                str(bar.volume),
            ),
        )
        self._conn.commit()

    def fetch_equity_bars(
        self,
        instrument_id: InstrumentId,
        timeframe: Timeframe,
        start_ts: str,
        end_ts: str,
    ) -> list[BarRow]:
        cur = self._conn.execute(
            """
            SELECT * FROM equity_bars
            WHERE instrument_id = ? AND timeframe = ?
              AND bar_ts_utc >= ? AND bar_ts_utc <= ?
            ORDER BY bar_ts_utc
            """,
            (str(instrument_id), timeframe.value, start_ts, end_ts),
        )
        return [_bar_from_row(r, "equity") for r in cur.fetchall()]

    def resolve_table_for_instrument(
        self, instrument_id: InstrumentId, asset_class: AssetClass
    ) -> str:
        if asset_class == AssetClass.EQUITY:
            return "equity_bars"
        if asset_class == AssetClass.FUTURE:
            return "futures_bars"
        if asset_class == AssetClass.OPTION:
            return "options_bars"
        return "equity_bars"


def _bar_from_row(row: sqlite3.Row, kind: str) -> BarRow:
    d = dict(row)
    return BarRow(
        instrument_id=InstrumentId(d["instrument_id"]),
        timeframe=d["timeframe"],
        bar_ts_utc=d["bar_ts_utc"],
        open=Decimal(d["open"]),
        high=Decimal(d["high"]),
        low=Decimal(d["low"]),
        close=Decimal(d["close"]),
        volume=Decimal(d["volume"]),
    )
