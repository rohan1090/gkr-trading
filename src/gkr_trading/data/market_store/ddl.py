"""SQLite DDL for instrument master, universes, native bars, derived scaffolding, events."""

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id TEXT PRIMARY KEY,
    asset_class TEXT NOT NULL,
    canonical_symbol TEXT NOT NULL,
    vendor_symbol TEXT,
    underlying_instrument_id TEXT,
    expiry TEXT,
    strike TEXT,
    right TEXT,
    contract_month TEXT,
    multiplier TEXT NOT NULL,
    exchange TEXT,
    currency TEXT NOT NULL,
    status TEXT NOT NULL,
    FOREIGN KEY (underlying_instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS universes (
    universe_name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS universe_members (
    universe_name TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    PRIMARY KEY (universe_name, instrument_id),
    FOREIGN KEY (universe_name) REFERENCES universes(universe_name),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS equity_bars (
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    bar_ts_utc TEXT NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY (instrument_id, timeframe, bar_ts_utc),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS futures_bars (
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    bar_ts_utc TEXT NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY (instrument_id, timeframe, bar_ts_utc),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS options_bars (
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    bar_ts_utc TEXT NOT NULL,
    open TEXT,
    high TEXT,
    low TEXT,
    close TEXT,
    volume TEXT,
    PRIMARY KEY (instrument_id, timeframe, bar_ts_utc),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS options_quotes (
    instrument_id TEXT NOT NULL,
    quote_ts_utc TEXT NOT NULL,
    bid TEXT,
    ask TEXT,
    bid_size TEXT,
    ask_size TEXT,
    implied_vol TEXT,
    PRIMARY KEY (instrument_id, quote_ts_utc),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS derived_continuous_futures (
    series_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    bar_ts_utc TEXT NOT NULL,
    roll_flag INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (series_id, bar_ts_utc),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS derived_option_chain_snapshots (
    snapshot_ts_utc TEXT NOT NULL,
    underlying_instrument_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (underlying_instrument_id, snapshot_ts_utc),
    FOREIGN KEY (underlying_instrument_id) REFERENCES instruments(instrument_id)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    envelope_json TEXT NOT NULL,
    UNIQUE(session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);

CREATE TABLE IF NOT EXISTS broker_reconciliation_state (
    session_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);
"""


def init_schema(conn) -> None:
    conn.executescript(DDL)
    conn.commit()
