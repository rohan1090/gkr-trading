"""Tests for the observation plane: caches, data bus, Schwab config, runtime pause."""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from gkr_trading.persistence.market_cache import MarketSnapshotCache
from gkr_trading.persistence.position_cache import PositionCache
from gkr_trading.live.data_bus import DataBus


# ── MarketSnapshotCache ────────────────────────────────────────────────

def _make_cache_conn() -> sqlite3.Connection:
    return sqlite3.connect(":memory:", check_same_thread=False)


class TestMarketSnapshotCache:
    def test_upsert_and_retrieve(self):
        conn = _make_cache_conn()
        cache = MarketSnapshotCache(conn)

        cache.upsert_snapshot("AAPL", {
            "last_cents": 21450,
            "open_cents": 21200,
            "source": "alpaca_snapshot",
        })

        snap = cache.get_snapshot("AAPL")
        assert snap is not None
        assert snap["last_cents"] == 21450
        assert snap["open_cents"] == 21200
        assert snap["source"] == "alpaca_snapshot"
        assert snap["updated_at"] is not None

        # get_all_snapshots
        all_snaps = cache.get_all_snapshots()
        assert "AAPL" in all_snaps
        assert all_snaps["AAPL"]["last_cents"] == 21450

    def test_upsert_preserves_existing_fields(self):
        conn = _make_cache_conn()
        cache = MarketSnapshotCache(conn)

        cache.upsert_snapshot("SPY", {"last_cents": 59100, "volume": 1000000})
        cache.upsert_snapshot("SPY", {"last_cents": 59200})

        snap = cache.get_snapshot("SPY")
        assert snap["last_cents"] == 59200
        assert snap["volume"] == 1000000  # preserved from first upsert

    def test_stale_detection(self):
        conn = _make_cache_conn()
        cache = MarketSnapshotCache(conn)

        # Insert with an old timestamp
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        conn.execute(
            "INSERT INTO market_snapshots (ticker, last_cents, updated_at) "
            "VALUES (?, ?, ?)",
            ("TSLA", 24500, old_ts),
        )
        conn.commit()

        stale = cache.get_tickers_with_stale_data(300)  # 5 minutes
        assert "TSLA" in stale

    def test_missing_ticker_returns_none(self):
        conn = _make_cache_conn()
        cache = MarketSnapshotCache(conn)
        assert cache.get_snapshot("NONEXISTENT") is None


# ── PositionCache ──────────────────────────────────────────────────────

class TestPositionCache:
    def test_upsert_and_clear_on_refresh(self):
        conn = _make_cache_conn()
        cache = PositionCache(conn)

        # Insert 3 positions
        cache.upsert_positions([
            {"ticker": "AAPL", "qty": 10, "side": "long"},
            {"ticker": "SPY", "qty": 5, "side": "long"},
            {"ticker": "TSLA", "qty": 3, "side": "long"},
        ], source="alpaca_paper")

        assert len(cache.get_positions(source="alpaca_paper")) == 3

        # Refresh with only 1 (simulates closed positions)
        cache.upsert_positions([
            {"ticker": "AAPL", "qty": 10, "side": "long"},
        ], source="alpaca_paper")

        positions = cache.get_positions(source="alpaca_paper")
        assert len(positions) == 1
        assert positions[0]["ticker"] == "AAPL"

    def test_account_upsert_and_get(self):
        conn = _make_cache_conn()
        cache = PositionCache(conn)

        cache.upsert_account({
            "cash_cents": 7492300,
            "portfolio_value_cents": 7500000,
        }, source="alpaca_paper")

        acct = cache.get_account()
        assert acct is not None
        assert acct["cash_cents"] == 7492300

    def test_get_last_updated(self):
        conn = _make_cache_conn()
        cache = PositionCache(conn)

        assert cache.get_last_updated("alpaca_paper") is None

        cache.upsert_positions([
            {"ticker": "AAPL", "qty": 10},
        ], source="alpaca_paper")

        ts = cache.get_last_updated("alpaca_paper")
        assert ts is not None
        assert isinstance(ts, datetime)

    def test_source_filter(self):
        conn = _make_cache_conn()
        cache = PositionCache(conn)

        cache.upsert_positions([{"ticker": "AAPL"}], source="alpaca_paper")
        cache.upsert_positions([{"ticker": "MSFT"}], source="schwab_live")

        alpaca = cache.get_positions(source="alpaca_paper")
        assert len(alpaca) == 1
        assert alpaca[0]["ticker"] == "AAPL"


# ── DataBus ────────────────────────────────────────────────────────────

class TestDataBus:
    def test_subscribe_and_publish(self):
        bus = DataBus()
        received = []

        bus.subscribe("test.topic", lambda p: received.append(p))
        bus.publish("test.topic", {"value": 42})

        assert len(received) == 1
        assert received[0]["value"] == 42

    def test_bad_callback_doesnt_block_others(self):
        bus = DataBus()
        received = []

        def bad_callback(p):
            raise ValueError("boom")

        bus.subscribe("test.topic", bad_callback)
        bus.subscribe("test.topic", lambda p: received.append(p))

        bus.publish("test.topic", {"value": 1})

        # Second callback should still have been called
        assert len(received) == 1
        assert received[0]["value"] == 1

    def test_multiple_subscribers(self):
        bus = DataBus()
        results = {"a": [], "b": [], "c": []}

        bus.subscribe("t", lambda p: results["a"].append(p))
        bus.subscribe("t", lambda p: results["b"].append(p))
        bus.subscribe("t", lambda p: results["c"].append(p))

        bus.publish("t", {"x": 1})

        assert len(results["a"]) == 1
        assert len(results["b"]) == 1
        assert len(results["c"]) == 1

    def test_unsubscribe(self):
        bus = DataBus()
        received = []
        cb = lambda p: received.append(p)

        bus.subscribe("t", cb)
        bus.publish("t", {})
        assert len(received) == 1

        bus.unsubscribe("t", cb)
        bus.publish("t", {})
        assert len(received) == 1  # not called again

    def test_publish_to_empty_topic(self):
        bus = DataBus()
        # Should not raise
        bus.publish("nonexistent.topic", {"data": True})


# ── Schwab Config ──────────────────────────────────────────────────────

class TestSchwabConfig:
    def test_raises_on_missing_env(self):
        from gkr_trading.live.schwab_adapter import SchwabConfig

        # Ensure env vars are absent
        for key in ["SCHWAB_ACCESS_TOKEN", "SCHWAB_REFRESH_TOKEN",
                     "SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET",
                     "SCHWAB_ACCOUNT_HASH"]:
            os.environ.pop(key, None)

        with pytest.raises(EnvironmentError, match="Missing Schwab env vars"):
            SchwabConfig.from_env()

    def test_is_schwab_available_returns_false(self):
        from gkr_trading.live.schwab_adapter import is_schwab_available

        for key in ["SCHWAB_ACCESS_TOKEN", "SCHWAB_REFRESH_TOKEN",
                     "SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET",
                     "SCHWAB_ACCOUNT_HASH"]:
            os.environ.pop(key, None)

        assert is_schwab_available() is False


# ── Runtime V2 pause config ───────────────────────────────────────────

class TestRuntimeV2PauseConfig:
    def test_pause_on_market_close_default_false(self):
        from gkr_trading.live.runtime_v2 import ContinuousSessionConfig

        cfg = ContinuousSessionConfig()
        # Default is False to preserve backward compatibility
        assert cfg.pause_on_market_close is False

    def test_pause_on_market_close_explicit_true(self):
        from gkr_trading.live.runtime_v2 import ContinuousSessionConfig

        cfg = ContinuousSessionConfig(pause_on_market_close=True)
        assert cfg.pause_on_market_close is True
