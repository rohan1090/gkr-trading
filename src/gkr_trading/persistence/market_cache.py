"""SQLite-backed market data cache — persists last known price snapshot per ticker.

The cache survives process restarts and serves as the fallback when
live API calls fail.  Thread-safe via internal ``threading.Lock``.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS market_snapshots (
    ticker            TEXT PRIMARY KEY,
    last_cents        INTEGER,
    open_cents        INTEGER,
    high_cents        INTEGER,
    low_cents         INTEGER,
    prev_close_cents  INTEGER,
    volume            INTEGER,
    vwap_cents        INTEGER,
    timestamp_utc     TEXT,
    source            TEXT,
    updated_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_updated_at
    ON market_snapshots(updated_at);
"""


class MarketSnapshotCache:
    """Persists the last known price snapshot for every ticker across restarts."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        try:
            self._conn.executescript(_SCHEMA)
        except Exception as exc:
            logger.error(f"MarketSnapshotCache schema init failed: {exc}")

    def upsert_snapshot(self, ticker: str, data: dict) -> None:
        """Upsert a snapshot.  *data* can contain any subset of column fields."""
        with self._lock:
            try:
                now = datetime.now(timezone.utc).isoformat()
                self._conn.execute(
                    """INSERT INTO market_snapshots
                       (ticker, last_cents, open_cents, high_cents, low_cents,
                        prev_close_cents, volume, vwap_cents, timestamp_utc,
                        source, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(ticker) DO UPDATE SET
                        last_cents       = COALESCE(excluded.last_cents,       market_snapshots.last_cents),
                        open_cents       = COALESCE(excluded.open_cents,       market_snapshots.open_cents),
                        high_cents       = COALESCE(excluded.high_cents,       market_snapshots.high_cents),
                        low_cents        = COALESCE(excluded.low_cents,        market_snapshots.low_cents),
                        prev_close_cents = COALESCE(excluded.prev_close_cents, market_snapshots.prev_close_cents),
                        volume           = COALESCE(excluded.volume,           market_snapshots.volume),
                        vwap_cents       = COALESCE(excluded.vwap_cents,       market_snapshots.vwap_cents),
                        timestamp_utc    = COALESCE(excluded.timestamp_utc,    market_snapshots.timestamp_utc),
                        source           = COALESCE(excluded.source,           market_snapshots.source),
                        updated_at       = excluded.updated_at
                    """,
                    (
                        ticker,
                        data.get("last_cents"),
                        data.get("open_cents"),
                        data.get("high_cents"),
                        data.get("low_cents"),
                        data.get("prev_close_cents"),
                        data.get("volume"),
                        data.get("vwap_cents"),
                        data.get("timestamp_utc"),
                        data.get("source"),
                        now,
                    ),
                )
                self._conn.commit()
            except Exception as exc:
                logger.error(f"MarketSnapshotCache upsert error for {ticker}: {exc}")

    def get_snapshot(self, ticker: str) -> Optional[dict]:
        """Return the cached snapshot for *ticker*, or ``None``."""
        try:
            cur = self._conn.execute(
                "SELECT * FROM market_snapshots WHERE ticker = ?", (ticker,)
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
        except Exception as exc:
            logger.error(f"MarketSnapshotCache get error for {ticker}: {exc}")
            return None

    def get_all_snapshots(self) -> dict[str, dict]:
        """Return all cached snapshots keyed by ticker."""
        try:
            cur = self._conn.execute("SELECT * FROM market_snapshots")
            cols = [desc[0] for desc in cur.description]
            result: dict[str, dict] = {}
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                result[d["ticker"]] = d
            return result
        except Exception as exc:
            logger.error(f"MarketSnapshotCache get_all error: {exc}")
            return {}

    def get_tickers_with_stale_data(self, stale_after_seconds: int) -> list[str]:
        """Return tickers whose ``updated_at`` is older than *stale_after_seconds*."""
        try:
            cutoff = datetime.now(timezone.utc).timestamp() - stale_after_seconds
            cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
            cur = self._conn.execute(
                "SELECT ticker FROM market_snapshots WHERE updated_at < ?",
                (cutoff_iso,),
            )
            return [row[0] for row in cur.fetchall()]
        except Exception as exc:
            logger.error(f"MarketSnapshotCache stale check error: {exc}")
            return []
