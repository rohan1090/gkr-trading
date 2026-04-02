from __future__ import annotations

import sqlite3

from gkr_trading.core.schemas.ids import InstrumentId
from gkr_trading.data.instrument_master.models import InstrumentRecord


class InstrumentRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def upsert(self, rec: InstrumentRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO instruments (
              instrument_id, asset_class, canonical_symbol, vendor_symbol,
              underlying_instrument_id, expiry, strike, right, contract_month,
              multiplier, exchange, currency, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(instrument_id) DO UPDATE SET
              asset_class=excluded.asset_class,
              canonical_symbol=excluded.canonical_symbol,
              vendor_symbol=excluded.vendor_symbol,
              underlying_instrument_id=excluded.underlying_instrument_id,
              expiry=excluded.expiry,
              strike=excluded.strike,
              right=excluded.right,
              contract_month=excluded.contract_month,
              multiplier=excluded.multiplier,
              exchange=excluded.exchange,
              currency=excluded.currency,
              status=excluded.status
            """,
            (
                str(rec.instrument_id),
                rec.asset_class.value,
                rec.canonical_symbol,
                rec.vendor_symbol,
                str(rec.underlying_instrument_id) if rec.underlying_instrument_id else None,
                rec.expiry.isoformat() if rec.expiry else None,
                str(rec.strike) if rec.strike is not None else None,
                rec.right.value if rec.right else None,
                rec.contract_month,
                str(rec.multiplier),
                rec.exchange,
                rec.currency,
                rec.status.value,
            ),
        )
        self._conn.commit()

    def get(self, instrument_id: InstrumentId) -> InstrumentRecord | None:
        row = self._conn.execute(
            "SELECT * FROM instruments WHERE instrument_id = ?",
            (str(instrument_id),),
        ).fetchone()
        if row is None:
            return None
        return _row_to_record(row)

    def list_all(self) -> list[InstrumentRecord]:
        cur = self._conn.execute("SELECT * FROM instruments ORDER BY canonical_symbol")
        return [_row_to_record(r) for r in cur.fetchall()]


def _row_to_record(row: sqlite3.Row) -> InstrumentRecord:
    from datetime import date
    from decimal import Decimal

    from gkr_trading.core.schemas.enums import AssetClass, InstrumentStatus, OptionRight

    d = {k: row[k] for k in row.keys()}
    return InstrumentRecord(
        instrument_id=InstrumentId(d["instrument_id"]),
        asset_class=AssetClass(d["asset_class"]),
        canonical_symbol=d["canonical_symbol"],
        vendor_symbol=d["vendor_symbol"],
        underlying_instrument_id=(
            InstrumentId(d["underlying_instrument_id"])
            if d["underlying_instrument_id"]
            else None
        ),
        expiry=date.fromisoformat(d["expiry"]) if d["expiry"] else None,
        strike=Decimal(d["strike"]) if d["strike"] else None,
        right=OptionRight(d["right"]) if d["right"] else None,
        contract_month=d["contract_month"],
        multiplier=Decimal(d["multiplier"]),
        exchange=d["exchange"],
        currency=d["currency"],
        status=InstrumentStatus(d["status"]),
    )
