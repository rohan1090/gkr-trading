from __future__ import annotations

import sqlite3

from gkr_trading.core.schemas.ids import InstrumentId


class UniverseRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def create_universe(self, name: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO universes(universe_name) VALUES (?)",
            (name,),
        )
        self._conn.commit()

    def add_member(self, universe_name: str, instrument_id: InstrumentId) -> None:
        self._conn.execute(
            """
            INSERT OR IGNORE INTO universe_members(universe_name, instrument_id)
            VALUES (?, ?)
            """,
            (universe_name, str(instrument_id)),
        )
        self._conn.commit()

    def members(self, universe_name: str) -> list[InstrumentId]:
        cur = self._conn.execute(
            "SELECT instrument_id FROM universe_members WHERE universe_name = ? ORDER BY instrument_id",
            (universe_name,),
        )
        return [InstrumentId(r[0]) for r in cur.fetchall()]
