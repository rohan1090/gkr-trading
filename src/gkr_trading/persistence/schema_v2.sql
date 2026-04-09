-- GKR Trading schema v2 — observation plane cache tables.
-- Idempotent: safe to run on an existing database.

-- Market snapshot cache: last known price per ticker, updated by
-- the observation plane on every poll cycle.  Persists across
-- restarts so the TUI can show prices immediately.
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

-- Position cache: live positions from venue APIs (Alpaca, Schwab).
-- Source-scoped: upsert_positions deletes all rows for a source
-- before inserting, so closed positions are removed automatically.
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

CREATE INDEX IF NOT EXISTS idx_position_cache_source
    ON position_cache(source);

-- Account cache: singleton row per source with cash, equity, P&L.
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
