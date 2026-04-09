"""Tests for Supabase cache layer — DualWrite fallback behavior."""
from __future__ import annotations

import os
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from gkr_trading.persistence.market_cache import MarketSnapshotCache
from gkr_trading.persistence.position_cache import PositionCache
from gkr_trading.persistence.supabase_cache import (
    DualWriteMarketCache,
    DualWritePositionCache,
    create_cache_layer,
)


class TestDualWriteFallback:
    def test_dual_write_market_falls_back_to_sqlite(self):
        """DualWriteMarketCache continues working when Supabase fails."""
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        sqlite_cache = MarketSnapshotCache(conn)

        # Mock a broken Supabase cache
        mock_supa = MagicMock()
        mock_supa.upsert_snapshot.side_effect = Exception("Supabase down")

        dual = DualWriteMarketCache(sqlite_cache, mock_supa)

        # Write should succeed (SQLite) even though Supabase fails
        dual.upsert_snapshot("AAPL", {"last_cents": 21450})

        # Read should work from SQLite
        snap = dual.get_snapshot("AAPL")
        assert snap is not None
        assert snap["last_cents"] == 21450

    def test_dual_write_positions_falls_back_to_sqlite(self):
        """DualWritePositionCache continues working when Supabase fails."""
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        sqlite_cache = PositionCache(conn)

        mock_supa = MagicMock()
        mock_supa.upsert_positions.side_effect = Exception("Supabase down")

        dual = DualWritePositionCache(sqlite_cache, mock_supa)

        dual.upsert_positions([{"ticker": "SPY", "qty": 5}], source="alpaca_paper")
        positions = dual.get_positions(source="alpaca_paper")
        assert len(positions) == 1
        assert positions[0]["ticker"] == "SPY"


class TestCreateCacheLayer:
    def test_returns_sqlite_when_no_supabase_env(self):
        """Without SUPABASE_URL, factory returns plain SQLite caches."""
        os.environ.pop("SUPABASE_URL", None)

        conn = sqlite3.connect(":memory:", check_same_thread=False)
        market, positions = create_cache_layer(conn)

        assert isinstance(market, MarketSnapshotCache)
        assert isinstance(positions, PositionCache)
