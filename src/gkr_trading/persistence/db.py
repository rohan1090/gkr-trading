from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gkr_trading.persistence.market_cache import MarketSnapshotCache
    from gkr_trading.persistence.position_cache import PositionCache


def open_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def open_sqlite_with_caches(
    db_path: str,
) -> tuple[sqlite3.Connection, "MarketSnapshotCache", "PositionCache"]:
    """Open SQLite with cache tables and optimised PRAGMAs.

    Returns ``(conn, market_cache, position_cache)``.
    """
    from gkr_trading.persistence.market_cache import MarketSnapshotCache
    from gkr_trading.persistence.position_cache import PositionCache

    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-64000")  # 64 MB

    market_cache = MarketSnapshotCache(conn)
    position_cache = PositionCache(conn)
    return conn, market_cache, position_cache
