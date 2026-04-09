"""Optional Supabase-backed caches and dual-write proxies.

Supabase is an optional dependency.  When unavailable, the factory
function :func:`create_cache_layer` returns plain SQLite caches.
When available and configured, it returns :class:`DualWriteMarketCache`
/ :class:`DualWritePositionCache` that write to both SQLite and Supabase
but always read from SQLite (fast local reads).

Required Supabase tables (create via SQL editor or migration):

.. code-block:: sql

    -- market_snapshots (same schema as SQLite version)
    CREATE TABLE IF NOT EXISTS market_snapshots (
        ticker            TEXT PRIMARY KEY,
        last_cents        INTEGER,
        open_cents        INTEGER, high_cents INTEGER,
        low_cents         INTEGER, prev_close_cents INTEGER,
        volume            INTEGER, vwap_cents INTEGER,
        timestamp_utc     TEXT, source TEXT, updated_at TEXT
    );

    -- position_cache
    CREATE TABLE IF NOT EXISTS position_cache (
        ticker TEXT PRIMARY KEY, qty REAL, side TEXT,
        avg_entry_cents INTEGER, last_cents INTEGER,
        unrealized_pnl_cents INTEGER, unrealized_pnl_pct REAL,
        market_value_cents INTEGER, cost_basis_cents INTEGER,
        asset_class TEXT, source TEXT, updated_at TEXT
    );

    -- account_cache
    CREATE TABLE IF NOT EXISTS account_cache (
        id INTEGER PRIMARY KEY, cash_cents INTEGER,
        portfolio_value_cents INTEGER, buying_power_cents INTEGER,
        equity_cents INTEGER, unrealized_pnl_cents INTEGER,
        source TEXT, updated_at TEXT
    );
"""
from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from gkr_trading.persistence.market_cache import MarketSnapshotCache
from gkr_trading.persistence.position_cache import PositionCache

logger = logging.getLogger(__name__)

# Optional import — supabase may not be installed
try:
    from supabase import create_client, Client as SupabaseClient  # type: ignore
    _SUPABASE_AVAILABLE = True
except ImportError:
    _SUPABASE_AVAILABLE = False
    SupabaseClient = Any  # type: ignore


# ── Config ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    key: str  # anon key or service key

    @classmethod
    def from_env(cls) -> SupabaseConfig:
        url = os.environ.get("SUPABASE_URL", "").strip()
        key = (
            os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
            or os.environ.get("SUPABASE_ANON_KEY", "").strip()
        )
        if not url or not key:
            raise EnvironmentError("Missing SUPABASE_URL or SUPABASE_ANON_KEY")
        return cls(url=url, key=key)


# ── Supabase Market Cache ──────────────────────────────────────────────

class SupabaseMarketCache:
    """Supabase-backed market snapshot cache.  Same interface as SQLite version."""

    def __init__(self, config: SupabaseConfig) -> None:
        if not _SUPABASE_AVAILABLE:
            raise ImportError("supabase package not installed")
        self._client: SupabaseClient = create_client(config.url, config.key)

    def upsert_snapshot(self, ticker: str, data: dict) -> None:
        try:
            row = {"ticker": ticker, **data}
            row["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._client.table("market_snapshots").upsert(row).execute()
        except Exception as exc:
            logger.warning(f"Supabase market upsert failed for {ticker}: {exc}")

    def get_snapshot(self, ticker: str) -> Optional[dict]:
        try:
            resp = (
                self._client.table("market_snapshots")
                .select("*")
                .eq("ticker", ticker)
                .execute()
            )
            if resp.data:
                return resp.data[0]
            return None
        except Exception as exc:
            logger.warning(f"Supabase market get failed for {ticker}: {exc}")
            return None

    def get_all_snapshots(self) -> dict[str, dict]:
        try:
            resp = self._client.table("market_snapshots").select("*").execute()
            return {row["ticker"]: row for row in (resp.data or [])}
        except Exception as exc:
            logger.warning(f"Supabase market get_all failed: {exc}")
            return {}

    def get_tickers_with_stale_data(self, stale_after_seconds: int) -> list[str]:
        try:
            cutoff = datetime.now(timezone.utc).timestamp() - stale_after_seconds
            cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
            resp = (
                self._client.table("market_snapshots")
                .select("ticker")
                .lt("updated_at", cutoff_iso)
                .execute()
            )
            return [row["ticker"] for row in (resp.data or [])]
        except Exception as exc:
            logger.warning(f"Supabase stale check failed: {exc}")
            return []


# ── Supabase Position Cache ────────────────────────────────────────────

class SupabasePositionCache:
    """Supabase-backed position + account cache."""

    def __init__(self, config: SupabaseConfig) -> None:
        if not _SUPABASE_AVAILABLE:
            raise ImportError("supabase package not installed")
        self._client: SupabaseClient = create_client(config.url, config.key)

    def upsert_positions(self, positions: list[dict], source: str) -> None:
        try:
            self._client.table("position_cache").delete().eq("source", source).execute()
            now = datetime.now(timezone.utc).isoformat()
            for pos in positions:
                row = {**pos, "source": source, "updated_at": now}
                self._client.table("position_cache").upsert(row).execute()
        except Exception as exc:
            logger.warning(f"Supabase positions upsert failed: {exc}")

    def get_positions(self, source: Optional[str] = None) -> list[dict]:
        try:
            q = self._client.table("position_cache").select("*")
            if source:
                q = q.eq("source", source)
            return q.execute().data or []
        except Exception as exc:
            logger.warning(f"Supabase positions get failed: {exc}")
            return []

    def upsert_account(self, account: dict, source: str) -> None:
        try:
            row = {"id": 1, **account, "source": source,
                   "updated_at": datetime.now(timezone.utc).isoformat()}
            self._client.table("account_cache").upsert(row).execute()
        except Exception as exc:
            logger.warning(f"Supabase account upsert failed: {exc}")

    def get_account(self, source: Optional[str] = None) -> Optional[dict]:
        try:
            resp = self._client.table("account_cache").select("*").eq("id", 1).execute()
            if resp.data:
                d = resp.data[0]
                if source and d.get("source") != source:
                    return None
                return d
            return None
        except Exception as exc:
            logger.warning(f"Supabase account get failed: {exc}")
            return None


# ── Dual-Write Proxies ─────────────────────────────────────────────────

class DualWriteMarketCache:
    """Writes to both SQLite and Supabase; reads from SQLite only."""

    def __init__(
        self, sqlite_cache: MarketSnapshotCache, supabase_cache: SupabaseMarketCache
    ) -> None:
        self._sqlite = sqlite_cache
        self._supa = supabase_cache

    def upsert_snapshot(self, ticker: str, data: dict) -> None:
        self._sqlite.upsert_snapshot(ticker, data)
        try:
            self._supa.upsert_snapshot(ticker, data)
        except Exception as exc:
            logger.debug(f"Supabase write-through failed (non-fatal): {exc}")

    def get_snapshot(self, ticker: str) -> Optional[dict]:
        return self._sqlite.get_snapshot(ticker)

    def get_all_snapshots(self) -> dict[str, dict]:
        return self._sqlite.get_all_snapshots()

    def get_tickers_with_stale_data(self, stale_after_seconds: int) -> list[str]:
        return self._sqlite.get_tickers_with_stale_data(stale_after_seconds)


class DualWritePositionCache:
    """Writes to both SQLite and Supabase; reads from SQLite only."""

    def __init__(
        self, sqlite_cache: PositionCache, supabase_cache: SupabasePositionCache
    ) -> None:
        self._sqlite = sqlite_cache
        self._supa = supabase_cache

    def upsert_positions(self, positions: list[dict], source: str) -> None:
        self._sqlite.upsert_positions(positions, source)
        try:
            self._supa.upsert_positions(positions, source)
        except Exception as exc:
            logger.debug(f"Supabase write-through failed (non-fatal): {exc}")

    def get_positions(self, source: Optional[str] = None) -> list[dict]:
        return self._sqlite.get_positions(source)

    def upsert_account(self, account: dict, source: str) -> None:
        self._sqlite.upsert_account(account, source)
        try:
            self._supa.upsert_account(account, source)
        except Exception as exc:
            logger.debug(f"Supabase write-through failed (non-fatal): {exc}")

    def get_account(self, source: Optional[str] = None) -> Optional[dict]:
        return self._sqlite.get_account(source)

    def get_last_updated(self, source: str) -> Optional[datetime]:
        return self._sqlite.get_last_updated(source)


# ── Factory ────────────────────────────────────────────────────────────

def create_cache_layer(
    db_conn: sqlite3.Connection,
) -> tuple[Any, Any]:
    """Return (market_cache, position_cache).

    DualWrite if Supabase is configured, plain SQLite otherwise.
    """
    sqlite_market = MarketSnapshotCache(db_conn)
    sqlite_positions = PositionCache(db_conn)

    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    if supabase_url and _SUPABASE_AVAILABLE:
        try:
            cfg = SupabaseConfig.from_env()
            supa_market = SupabaseMarketCache(cfg)
            supa_positions = SupabasePositionCache(cfg)
            logger.info("Cache layer: DualWrite (SQLite + Supabase)")
            return (
                DualWriteMarketCache(sqlite_market, supa_market),
                DualWritePositionCache(sqlite_positions, supa_positions),
            )
        except Exception as exc:
            logger.warning(f"Supabase init failed — using SQLite only: {exc}")

    logger.info("Cache layer: SQLite only")
    return sqlite_market, sqlite_positions
