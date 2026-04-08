"""Position store — SQLite-backed persistence for equity + options positions."""
from __future__ import annotations

import json
import sqlite3
import time
from typing import List, Optional

_DDL = """\
CREATE TABLE IF NOT EXISTS equity_positions (
    ticker          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    signed_qty      INTEGER NOT NULL,
    cost_basis_cents INTEGER NOT NULL DEFAULT 0,
    realized_pnl_cents INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'open',
    updated_at_ns   INTEGER NOT NULL,
    PRIMARY KEY (ticker, venue, session_id)
);

CREATE TABLE IF NOT EXISTS options_positions (
    occ_symbol      TEXT NOT NULL,
    venue           TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    instrument_ref_json TEXT NOT NULL,
    long_contracts  INTEGER NOT NULL DEFAULT 0,
    short_contracts INTEGER NOT NULL DEFAULT 0,
    long_premium_paid_cents INTEGER NOT NULL DEFAULT 0,
    short_premium_received_cents INTEGER NOT NULL DEFAULT 0,
    realized_pnl_cents INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'open',
    has_undefined_risk INTEGER NOT NULL DEFAULT 0,
    updated_at_ns   INTEGER NOT NULL,
    PRIMARY KEY (occ_symbol, venue, session_id)
);
"""


class PositionStore:
    """SQLite-backed position persistence for equity and options."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._conn.executescript(_DDL)

    # --- Equity ---

    def upsert_equity(
        self,
        *,
        ticker: str,
        venue: str,
        session_id: str,
        signed_qty: int,
        cost_basis_cents: int,
        realized_pnl_cents: int,
        status: str = "open",
    ) -> None:
        now_ns = time.time_ns()
        self._conn.execute(
            """INSERT INTO equity_positions
               (ticker, venue, session_id, signed_qty, cost_basis_cents,
                realized_pnl_cents, status, updated_at_ns)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(ticker, venue, session_id) DO UPDATE SET
                 signed_qty = excluded.signed_qty,
                 cost_basis_cents = excluded.cost_basis_cents,
                 realized_pnl_cents = excluded.realized_pnl_cents,
                 status = excluded.status,
                 updated_at_ns = excluded.updated_at_ns""",
            (ticker, venue, session_id, signed_qty, cost_basis_cents,
             realized_pnl_cents, status, now_ns),
        )
        self._conn.commit()

    def get_equity_positions(self, session_id: str, venue: str) -> List[dict]:
        cur = self._conn.execute(
            """SELECT ticker, signed_qty, cost_basis_cents, realized_pnl_cents, status
               FROM equity_positions WHERE session_id = ? AND venue = ?""",
            (session_id, venue),
        )
        return [
            {"ticker": r[0], "signed_qty": r[1], "cost_basis_cents": r[2],
             "realized_pnl_cents": r[3], "status": r[4]}
            for r in cur.fetchall()
        ]

    # --- Options ---

    def upsert_options(
        self,
        *,
        occ_symbol: str,
        venue: str,
        session_id: str,
        instrument_ref_json: str,
        long_contracts: int,
        short_contracts: int,
        long_premium_paid_cents: int,
        short_premium_received_cents: int,
        realized_pnl_cents: int,
        status: str = "open",
        has_undefined_risk: bool = False,
    ) -> None:
        now_ns = time.time_ns()
        self._conn.execute(
            """INSERT INTO options_positions
               (occ_symbol, venue, session_id, instrument_ref_json,
                long_contracts, short_contracts, long_premium_paid_cents,
                short_premium_received_cents, realized_pnl_cents, status,
                has_undefined_risk, updated_at_ns)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(occ_symbol, venue, session_id) DO UPDATE SET
                 long_contracts = excluded.long_contracts,
                 short_contracts = excluded.short_contracts,
                 long_premium_paid_cents = excluded.long_premium_paid_cents,
                 short_premium_received_cents = excluded.short_premium_received_cents,
                 realized_pnl_cents = excluded.realized_pnl_cents,
                 status = excluded.status,
                 has_undefined_risk = excluded.has_undefined_risk,
                 updated_at_ns = excluded.updated_at_ns""",
            (occ_symbol, venue, session_id, instrument_ref_json,
             long_contracts, short_contracts, long_premium_paid_cents,
             short_premium_received_cents, realized_pnl_cents, status,
             int(has_undefined_risk), now_ns),
        )
        self._conn.commit()

    def get_options_positions(self, session_id: str, venue: str) -> List[dict]:
        cur = self._conn.execute(
            """SELECT occ_symbol, instrument_ref_json, long_contracts, short_contracts,
                      long_premium_paid_cents, short_premium_received_cents,
                      realized_pnl_cents, status, has_undefined_risk
               FROM options_positions WHERE session_id = ? AND venue = ?""",
            (session_id, venue),
        )
        return [
            {"occ_symbol": r[0], "instrument_ref_json": r[1],
             "long_contracts": r[2], "short_contracts": r[3],
             "long_premium_paid_cents": r[4], "short_premium_received_cents": r[5],
             "realized_pnl_cents": r[6], "status": r[7],
             "has_undefined_risk": bool(r[8])}
            for r in cur.fetchall()
        ]

    def remove_options_position(
        self, occ_symbol: str, venue: str, session_id: str
    ) -> None:
        """Remove an options position (e.g., after expiration)."""
        self._conn.execute(
            """UPDATE options_positions SET status = 'expired'
               WHERE occ_symbol = ? AND venue = ? AND session_id = ?""",
            (occ_symbol, venue, session_id),
        )
        self._conn.commit()
