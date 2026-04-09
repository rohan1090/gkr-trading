"""SQLite-backed position and account cache for always-on display.

Positions are stored per-source (``alpaca_paper``, ``schwab_live``, etc.).
When a source refreshes, all old rows for that source are replaced so that
closed positions (which disappear from the API) are correctly removed.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS position_cache (
    ticker                TEXT PRIMARY KEY,
    qty                   REAL,
    side                  TEXT,
    avg_entry_cents       INTEGER,
    last_cents            INTEGER,
    unrealized_pnl_cents  INTEGER,
    unrealized_pnl_pct    REAL,
    market_value_cents    INTEGER,
    cost_basis_cents      INTEGER,
    asset_class           TEXT,
    source                TEXT,
    updated_at            TEXT
);
CREATE INDEX IF NOT EXISTS idx_position_cache_source ON position_cache(source);

CREATE TABLE IF NOT EXISTS account_cache (
    id                     INTEGER PRIMARY KEY CHECK (id = 1),
    cash_cents             INTEGER,
    portfolio_value_cents  INTEGER,
    buying_power_cents     INTEGER,
    equity_cents           INTEGER,
    unrealized_pnl_cents   INTEGER,
    source                 TEXT,
    updated_at             TEXT
);
"""


class PositionCache:
    """Caches venue positions and account data across process restarts."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            self._conn.executescript(_SCHEMA)
        except Exception as exc:
            logger.error(f"PositionCache schema init failed: {exc}")

    # ── Positions ───────────────────────────────────────────────────

    def upsert_positions(self, positions: list[dict], source: str) -> None:
        """Replace all positions for *source* atomically."""
        with self._lock:
            try:
                now = datetime.now(timezone.utc).isoformat()
                self._conn.execute(
                    "DELETE FROM position_cache WHERE source = ?", (source,)
                )
                for pos in positions:
                    self._conn.execute(
                        """INSERT INTO position_cache
                           (ticker, qty, side, avg_entry_cents, last_cents,
                            unrealized_pnl_cents, unrealized_pnl_pct,
                            market_value_cents, cost_basis_cents,
                            asset_class, source, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            pos.get("ticker", ""),
                            pos.get("qty", 0),
                            pos.get("side", "long"),
                            pos.get("avg_entry_cents", 0),
                            pos.get("last_cents", 0),
                            pos.get("unrealized_pnl_cents", 0),
                            pos.get("unrealized_pnl_pct", 0.0),
                            pos.get("market_value_cents", 0),
                            pos.get("cost_basis_cents", 0),
                            pos.get("asset_class", "us_equity"),
                            source,
                            now,
                        ),
                    )
                self._conn.commit()
            except Exception as exc:
                logger.error(f"PositionCache upsert error: {exc}")

    def get_positions(self, source: Optional[str] = None) -> list[dict]:
        """Return cached positions, optionally filtered by *source*."""
        try:
            if source:
                cur = self._conn.execute(
                    "SELECT * FROM position_cache WHERE source = ?", (source,)
                )
            else:
                cur = self._conn.execute("SELECT * FROM position_cache")
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as exc:
            logger.error(f"PositionCache get error: {exc}")
            return []

    # ── Account ─────────────────────────────────────────────────────

    def upsert_account(self, account: dict, source: str) -> None:
        """Upsert the singleton account row."""
        with self._lock:
            try:
                now = datetime.now(timezone.utc).isoformat()
                self._conn.execute(
                    """INSERT INTO account_cache
                       (id, cash_cents, portfolio_value_cents, buying_power_cents,
                        equity_cents, unrealized_pnl_cents, source, updated_at)
                       VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET
                        cash_cents             = excluded.cash_cents,
                        portfolio_value_cents   = excluded.portfolio_value_cents,
                        buying_power_cents      = excluded.buying_power_cents,
                        equity_cents            = excluded.equity_cents,
                        unrealized_pnl_cents    = excluded.unrealized_pnl_cents,
                        source                  = excluded.source,
                        updated_at              = excluded.updated_at
                    """,
                    (
                        account.get("cash_cents", 0),
                        account.get("portfolio_value_cents", 0),
                        account.get("buying_power_cents", 0),
                        account.get("equity_cents", 0),
                        account.get("unrealized_pnl_cents", 0),
                        source,
                        now,
                    ),
                )
                self._conn.commit()
            except Exception as exc:
                logger.error(f"PositionCache upsert_account error: {exc}")

    def get_account(self, source: Optional[str] = None) -> Optional[dict]:
        """Return the cached account summary."""
        try:
            cur = self._conn.execute("SELECT * FROM account_cache WHERE id = 1")
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
            d = dict(zip(cols, row))
            if source and d.get("source") != source:
                return None
            return d
        except Exception as exc:
            logger.error(f"PositionCache get_account error: {exc}")
            return None

    def get_last_updated(self, source: str) -> Optional[datetime]:
        """Most recent ``updated_at`` for any position from *source*."""
        try:
            cur = self._conn.execute(
                "SELECT MAX(updated_at) FROM position_cache WHERE source = ?",
                (source,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
            return None
        except Exception as exc:
            logger.error(f"PositionCache get_last_updated error: {exc}")
            return None
