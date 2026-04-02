from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import sqlite3

from gkr_trading.core.schemas.enums import AssetClass, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId
from gkr_trading.data.instrument_master.repository import InstrumentRepository
from gkr_trading.data.market_store.repository import BarRow, MarketDataRepository
from gkr_trading.data.universe_registry.repository import UniverseRepository


@dataclass(frozen=True)
class HistoricalBarQuery:
    universe_name: str | None
    instrument_ids: list[InstrumentId] | None
    timeframe: Timeframe
    start_ts_utc: str
    end_ts_utc: str
    asset_class: AssetClass | None = None
    expiry_on_or_after: str | None = None
    strike_min: str | None = None
    strike_max: str | None = None
    option_right: str | None = None


class HistoricalDataRepository(Protocol):
    def fetch_bars(self, q: HistoricalBarQuery) -> list[BarRow]: ...


class DataAccessAPI:
    """Date-bounded, universe-scoped historical access (native store only)."""

    def __init__(
        self,
        conn: sqlite3.Connection,
    ) -> None:
        self._conn = conn
        self._inst = InstrumentRepository(conn)
        self._univ = UniverseRepository(conn)
        self._bars = MarketDataRepository(conn)

    def fetch_bars(self, q: HistoricalBarQuery) -> list[BarRow]:
        ids: list[InstrumentId] = []
        if q.universe_name:
            ids.extend(self._univ.members(q.universe_name))
        if q.instrument_ids:
            ids.extend(q.instrument_ids)
        if not ids:
            return []
        seen: set[str] = set()
        unique: list[InstrumentId] = []
        for i in ids:
            s = str(i)
            if s not in seen:
                seen.add(s)
                unique.append(i)

        out: list[BarRow] = []
        for iid in unique:
            rec = self._inst.get(iid)
            if rec is None:
                continue
            if q.asset_class is not None and rec.asset_class != q.asset_class:
                continue
            if q.expiry_on_or_after and rec.expiry:
                if rec.expiry.isoformat() < q.expiry_on_or_after:
                    continue
            if q.strike_min and rec.strike is not None and str(rec.strike) < q.strike_min:
                continue
            if q.strike_max and rec.strike is not None and str(rec.strike) > q.strike_max:
                continue
            if q.option_right and rec.right and rec.right.value != q.option_right:
                continue

            table = self._bars.resolve_table_for_instrument(iid, rec.asset_class)
            rows = self._conn.execute(
                f"""
                SELECT * FROM {table}
                WHERE instrument_id = ? AND timeframe = ?
                  AND bar_ts_utc >= ? AND bar_ts_utc <= ?
                ORDER BY bar_ts_utc
                """,
                (str(iid), q.timeframe.value, q.start_ts_utc, q.end_ts_utc),
            ).fetchall()
            for r in rows:
                out.append(
                    BarRow(
                        instrument_id=InstrumentId(r["instrument_id"]),
                        timeframe=r["timeframe"],
                        bar_ts_utc=r["bar_ts_utc"],
                        open=_dec(r["open"]),
                        high=_dec(r["high"]),
                        low=_dec(r["low"]),
                        close=_dec(r["close"]),
                        volume=_dec(r["volume"] or "0"),
                    )
                )
        out.sort(key=lambda b: (b.bar_ts_utc, str(b.instrument_id)))
        return out


def _dec(x: str) -> __import__("decimal").Decimal:
    from decimal import Decimal

    return Decimal(x)
