# GKR Trading Codebase Audit Report

**Date:** 2026-04-09
**Codebase:** `gkr-trading-6c1ecf87`
**Total Python files:** 149

---

## File-by-File Findings

### 1. `src/gkr_trading/live/runtime_v2.py` (777 lines)

The core runtime for continuous paper trading sessions.

**Dataclasses:**

| Class | Key Fields |
|-------|-----------|
| `PaperSessionV2Config` | `session_id`, `venue`, `mode`, `shadow_mode`, `dry_run` |
| `PaperSessionV2Result` | `session_id`, `events_count`, `fills_count`, `intents_generated/approved/rejected`, `orders_submitted/failed`, `startup_clean`, `shutdown_clean`, `replay_valid`, `shadow_mode`, `errors` |
| `StrategyContext` | `session_id`, `venue`, `kill_switch_level`, `is_shadow` |
| `ContinuousSessionConfig` | `poll_interval_sec=15.0`, `max_cycles=None`, **`stop_after_market_close=True`**, `max_consecutive_md_failures=5`, `enable_websocket=True` |
| `ContinuousSessionResult` | `session_result`, `cycles_completed`, `stop_reason`, `md_polls`, `md_envelopes`, `ws_connected`, `ws_trade_updates`, `replay_anomaly_count` |

**StopReason constants (line 538):**
```python
class StopReason:
    MARKET_CLOSED = "market_closed"
    KILL_SWITCH = "kill_switch"
    BLOCKING_RECON = "blocking_reconciliation_break"
    MD_FAILURE = "market_data_failure"
    MAX_CYCLES = "max_cycles_reached"
    OPERATOR_HALT = "operator_halt"
    WS_FATAL = "websocket_fatal"
    EXTERNAL = "external_stop"
```

**Market-close termination code (lines 695-709):**
```python
def _check_stop_conditions(self) -> Optional[str]:
    """Check all stop conditions. Returns reason or None."""
    # Kill switch
    if self._runner.supervisor.state == SessionState.HALTED:
        return StopReason.KILL_SWITCH

    # Market closed
    if self._config.stop_after_market_close and self._metadata:
        try:
            if not self._metadata.is_market_open():
                return StopReason.MARKET_CLOSED
        except Exception:
            pass  # Don't halt on metadata errors

    return None
```

**Termination flow:**
1. `_run_loop()` (line 655) calls `_check_stop_conditions()` each iteration (line 659).
2. If `stop_after_market_close=True` and `is_market_open()` returns `False`, returns `StopReason.MARKET_CLOSED`.
3. Propagates to `run_session()` (line 629) which stops WebSocket, calls `self._runner.shutdown()`, runs replay validation.
4. Metadata errors are silently swallowed (`except Exception: pass`), so a failed clock check will NOT halt the session.

**Key classes:**
- `PaperSessionRunnerV2` (lines 104-484): Full pipeline — strategy -> risk -> submit -> fill processing -> NTA handling.
- `ContinuousSessionRunner` (lines 562-776): Lifecycle wrapper with poll loop, WebSocket integration, reconnect reconciliation.
- `build_paper_runner()` (lines 486-521): Factory wiring all dependencies from a `sqlite3.Connection`.

---

### 2. `src/gkr_trading/live/market_data_feed.py` (276 lines)

**MarketDataFeedConfig dataclass (line 30):**
```python
@dataclass
class MarketDataFeedConfig:
    equity_tickers: Sequence[str] = ()
    options_occ_symbols: Sequence[str] = ()
    poll_interval_sec: float = 15.0
    max_consecutive_failures: int = 5
    stale_threshold_sec: float = 120.0
    drop_stale: bool = True  # False for TUI (always show latest data even if unchanged)
```

**Snapshot polling:**
- Method: `_poll_equity_snapshots()` (line 108)
- Endpoint: **`GET /v2/stocks/snapshots?symbols=...`**
- **No fallback to bars.** Within the snapshot response, prefers `minuteBar` over `dailyBar` for OHLCV, but this is field-level preference, not an API fallback.

**Stale-data filter (equity, lines 148-158):**
```python
trade_ts = latest_trade.get("t", "")
ts_key = f"equity:{ticker}"
ts_ns = _parse_rfc3339_ns(trade_ts) if trade_ts else now_ns

if ts_key in self._last_timestamps and self._last_timestamps[ts_key] == ts_ns:
    self._stats.stale_skips += 1
    if getattr(self._config, 'drop_stale', True):
        return None
self._last_timestamps[ts_key] = ts_ns
```

- Equity stale filter respects `drop_stale` config (TUI passes `False` to always show data).
- Options stale filter (line 223) always drops stale data unconditionally — no `drop_stale` check.

**Key class:** `AlpacaMarketDataFeed` — constructor takes `http_client` and `MarketDataFeedConfig`.

---

### 3. `src/gkr_trading/live/market_metadata_provider.py` (209 lines)

**Abstract base:** `MarketMetadataProvider(ABC)` with methods: `is_market_open()`, `next_market_open()`, `next_market_close()`, `minutes_until_close()`, `is_tradeable()`, `is_in_expiry_window()`.

**Concrete implementation:** `AlpacaMarketMetadataProvider`
- Constructor: `__init__(self, http_client, *, clock_cache_ttl_sec=60.0)`
- All methods call `_fetch_clock()` which hits **`GET /v2/clock`** (Alpaca Trading API).
- Clock response is cached with configurable TTL (default 60s).

**`is_market_open()` (line 94):**
```python
def is_market_open(self) -> bool:
    clock = self._fetch_clock()
    return clock.get("is_open", False)
```

**`is_tradeable()` (line 122):**
- Returns `False` if market is closed.
- For options on expiry day, restricts trading within 15 minutes of close.

**`next_market_open()` (line 98):** Parses `next_open` from clock response.

**Helper class:** `ExpiryWindowHalt` (line 155) — blocks options trading within a configurable window of expiry.

---

### 4. `src/gkr_trading/live/alpaca_config.py` (34 lines)

```python
DEFAULT_PAPER_BASE_URL = "https://paper-api.alpaca.markets"

@dataclass(frozen=True)
class AlpacaPaperConfig:
    api_key: str
    secret_key: str
    base_url: str = DEFAULT_PAPER_BASE_URL
```

**`from_env()` environment variables:**

| Env Var | Required | Default |
|---------|----------|---------|
| `ALPACA_API_KEY` | **Yes** | Raises `AlpacaConfigError` if empty |
| `ALPACA_SECRET_KEY` | **Yes** | Raises `AlpacaConfigError` if empty |
| `ALPACA_PAPER_BASE_URL` | No | `"https://paper-api.alpaca.markets"` |

Trailing `/` is stripped from `base_url`. All values are `.strip()`ed.

---

### 5. `src/gkr_trading/live/alpaca_http.py` (109 lines)

**Protocol:** `AlpacaHttpClient` with single method `request_json()`.

**`UrllibAlpacaHttpClient` (dataclass):**
- Constructor fields: `config: AlpacaPaperConfig`, `timeout_sec: float = 30.0`
- `request_json(self, method: str, path: str, *, query=None, json_body=None) -> Any`

**Auth headers:** `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY`.

**Exception hierarchy:**
| Exception | Purpose |
|-----------|---------|
| `AlpacaHttpError` | Non-2xx response (has `status_code`, `body_text`, `parsed`) |
| `AlpacaSubmitUncertaintyError` | Timeout/connection failure (has `client_order_id`) |
| `AlpacaSubmitUnresolvedError` | Post-uncertainty search found no order |

Uses `urllib.request` (stdlib) — no third-party HTTP library.

---

### 6. `src/gkr_trading/live/broker_adapter.py` (270 lines)

**`BrokerAdapter` Protocol (line 119):**
```python
class BrokerAdapter(Protocol):
    def submit(self, req: SubmitRequest) -> SubmissionResult: ...
    def poll_broker_facts(self, *, cursor, hints, phase) -> BrokerPollResult: ...
```

**Supporting types:**

| Type | Fields |
|------|--------|
| `SubmitRequest` | `order_id`, `instrument_id`, `side`, `quantity`, `order_type`, `limit_price`, `executable_broker_symbol`, `context_ts_utc` |
| `SubmissionResult` | `occurred_at_utc`, `broker_order_id`, `rejected`, `reject_reason_code`, `reject_reason_detail` |
| `BrokerFillFact` | `client_order_id`, `instrument_id`, `side`, `quantity`, `price`, `fees`, `fill_ts_utc`, `occurred_at_utc`, `broker_execution_id` |
| `BrokerOrderRejectedFact` | `client_order_id`, `reason_code`, `occurred_at_utc`, `reason_detail` |
| `BrokerOrderCancelledFact` | `client_order_id`, `occurred_at_utc`, `reason_code`, `cancelled_qty` |
| `BrokerPollResult` | `facts: tuple[BrokerFact, ...]`, `cursor: BrokerReconciliationCursor` |
| `BrokerSyncPhase` | Enum: `STARTUP`, `PRE_BAR`, `POST_SUBMIT`, `POST_BAR` |

**`MockBrokerAdapter`** (line 131): Deterministic paper broker for testing, with configurable fill deferral and duplicate fill injection.

---

### 7. `src/gkr_trading/live/session_supervisor.py` (226 lines)

**`SessionSupervisor` — lifecycle owner for trading sessions.**

**Constructor:**
- `event_store: EventStore`
- `pending_registry: PendingOrderRegistry`
- `reconciliation_service: ReconciliationService`
- `session_id: Optional[str] = None` (auto-generated UUID)
- `venue: str = "unknown"`

**State machine:** `SessionState` enum: `INITIALIZING` -> `RECONCILING_STARTUP` -> `RUNNING` -> `SUSPENDED` / `RECONCILING_SHUTDOWN` -> `HALTED` / `STOPPED`.

**Key methods:**
| Method | Purpose |
|--------|---------|
| `startup()` -> `bool` | Marks non-terminal orders as UNKNOWN, runs startup reconciliation, checks for blocking breaks |
| `activate_kill_switch(level)` | Sets kill switch; FULL_HALT transitions to HALTED |
| `suspend(reason)` | RUNNING -> SUSPENDED (e.g., WS disconnect) |
| `resume()` -> `bool` | Runs post-reconnect reconciliation, returns to RUNNING |
| `shutdown()` | Runs shutdown reconciliation, emits SESSION_STOPPED |
| `can_submit_orders()` | False if not RUNNING or FULL_HALT |
| `can_submit_new_orders()` | False if CLOSE_ONLY (closing orders still allowed) |

---

### 8. `src/gkr_trading/live/reconciliation_service.py` (163 lines)

**Constructor:**
- `position_store: PositionStore`
- `adapter: VenueAdapter`
- `session_id: str`

**`reconcile(trigger="on_demand")` method:**
1. Fetches venue positions + account.
2. Compares equity quantities (local vs venue). Mismatches where `local_qty != 0` are `"blocking"`; where `local_qty == 0` (pre-existing) are `"warning"`.
3. Compares options positions (net = long - short).
4. Cash reconciliation (cost_basis vs venue cash) — always `"warning"`.
5. Orphan order detection via `get_open_orders()` — `"warning"`.
6. Returns `OptionsReconciliationSnapshot` with status `"clean"` or `"break_detected"`.

---

### 9. `src/gkr_trading/live/websocket_manager.py` (312 lines)

**`AlpacaWebSocketManager` — real-time Alpaca `trade_updates` streaming.**

**Constructor:**
- `api_key: str`, `secret_key: str`
- `ws_url: str = ALPACA_TRADE_UPDATES_URL`
- Callbacks: `on_trade_update`, `on_connect`, `on_disconnect`, `on_reconnect`
- `max_reconnect_attempts: int = 50`
- `initial_backoff_sec: float = 1.0`, `max_backoff_sec: float = 60.0`
- `heartbeat_interval_sec: float = 30.0`, `heartbeat_timeout_sec: float = 90.0`

**Lifecycle:** `start()` spawns daemon thread -> `_connection_loop()` with exponential backoff -> `_connect_and_subscribe()` (SSL, auth, subscribe to `trade_updates`) -> `_receive_loop()`.

**State machine:** `ConnectionState`: `DISCONNECTED`, `CONNECTING`, `AUTHENTICATING`, `CONNECTED`, `RECONNECTING`, `CLOSED`.

---

### 10. `src/gkr_trading/persistence/db.py` (9 lines)

```python
def open_sqlite(path: str) -> sqlite3.Connection:
```

**PRAGMAs set: NONE.** Only sets `conn.row_factory = sqlite3.Row`. WAL mode is set separately in `event_store.py` via `enforce_wal_mode()`.

---

### 11. `src/gkr_trading/persistence/position_store.py` (157 lines)

**Constructor:** `__init__(self, conn: sqlite3.Connection)` — creates tables via DDL.

**Tables:**

**`equity_positions`:**
| Column | Type | Notes |
|--------|------|-------|
| `ticker` | TEXT | PK part |
| `venue` | TEXT | PK part |
| `session_id` | TEXT | PK part |
| `signed_qty` | INTEGER | |
| `cost_basis_cents` | INTEGER | DEFAULT 0 |
| `realized_pnl_cents` | INTEGER | DEFAULT 0 |
| `status` | TEXT | DEFAULT 'open' |
| `updated_at_ns` | INTEGER | |

**`options_positions`:**
| Column | Type | Notes |
|--------|------|-------|
| `occ_symbol` | TEXT | PK part |
| `venue` | TEXT | PK part |
| `session_id` | TEXT | PK part |
| `instrument_ref_json` | TEXT | |
| `long_contracts` | INTEGER | DEFAULT 0 |
| `short_contracts` | INTEGER | DEFAULT 0 |
| `long_premium_paid_cents` | INTEGER | DEFAULT 0 |
| `short_premium_received_cents` | INTEGER | DEFAULT 0 |
| `realized_pnl_cents` | INTEGER | DEFAULT 0 |
| `status` | TEXT | DEFAULT 'open' |
| `has_undefined_risk` | INTEGER | DEFAULT 0 |
| `updated_at_ns` | INTEGER | |

**Methods:** `upsert_equity()`, `get_equity_positions()`, `upsert_options()`, `get_options_positions()`, `remove_options_position()` (soft-delete to `status='expired'`).

---

### 12. `src/gkr_trading/persistence/event_store.py` (64 lines)

**`events` table schema:**
```sql
CREATE TABLE IF NOT EXISTS events (
    session_id   TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    envelope_json TEXT NOT NULL,
    PRIMARY KEY (session_id, seq)
);
```

**`enforce_wal_mode()` (line 18):**
```python
def enforce_wal_mode(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
```

**`SqliteEventStore`:**
- Constructor: `__init__(self, conn, *, init_schema=True)` — always calls `enforce_wal_mode()`, optionally creates schema.
- `append(session_id, event)` -> `int` (new seq number)
- `load_session(session_id)` -> `list[CanonicalEvent]`
- `max_seq(session_id)` -> `int`

---

### 13. `src/gkr_trading/persistence/pending_order_registry.py` (183 lines)

**Table: `pending_orders`**
```sql
CREATE TABLE IF NOT EXISTS pending_orders (
    client_order_id TEXT PRIMARY KEY,
    intent_id       TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    instrument_ref_json TEXT NOT NULL,
    action          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending_local',
    venue_order_id  TEXT,
    quantity        INTEGER NOT NULL,
    limit_price_cents INTEGER,
    created_at_ns   INTEGER NOT NULL,
    updated_at_ns   INTEGER NOT NULL
);
```
Indexes: `idx_po_session(session_id)`, `idx_po_status(status)`.

**Methods:** `register()`, `update_status()`, `mark_all_non_terminal_as_unknown()`, `get_unknown_orders()`, `get_active_orders()`, `exists()`, `get_status()`.

Terminal statuses: `{FILLED, CANCELED, REJECTED, EXPIRED}`.

---

### 14. `src/gkr_trading/tui/app.py`

**`GKRTradingApp.__init__` params:**
- `db_path: str` — SQLite database path
- `initial_session_id: Optional[str] = None`

**`on_mount` flow:**
1. Validates `db_path` exists.
2. Creates `DBWatcher(db_path)`, `MarketPoller()`, `AlpacaPositionsWorker()`.
3. Warns if market poller or positions worker unavailable (no Alpaca credentials).
4. Pushes `MainScreen()`.
5. Deferred `_initial_load` (0.5s timer): refreshes sessions, auto-discovers today's session, refreshes strategy/history panels.
6. Starts 3 background workers.

**Worker threads:**

| Worker | Method | Interval | Function |
|--------|--------|----------|----------|
| `db-watcher` | `_db_poll_loop` | 3.0s | Polls `DBWatcher.list_sessions()` and `poll_events()` |
| `market-poller` | `_market_poll_loop` | 15.0s | Polls `MarketPoller.poll_once()`, updates prices/status |
| `positions-poller` | `_positions_poll_loop` | 10.0s | Polls `AlpacaPositionsWorker.poll_once()`, updates positions/account |

---

### 15. `src/gkr_trading/tui/workers/market_poller.py`

**`MarketPoller.__init__`:**
- `equity_tickers: Sequence[str] | None = None` — reads `ALPACA_WATCHLIST` env var if None; falls back to `("AAPL", "SPY", "QQQ", "TSLA", "NVDA", "MSFT")`
- `poll_interval_sec: float = 15.0`

**`_init()`:** Creates two HTTP clients:
1. Broker client -> `paper-api.alpaca.markets` (for market status `/v2/clock`)
2. Data client -> `data.alpaca.markets` (for snapshots `/v2/stocks/snapshots`)

**`poll_once()` -> `tuple[list[MarketDataSnapshot], Optional[bool]]`:**
Returns `(snapshots, market_open_or_None)`. Maintains per-ticker `_price_history` (capped at 120 points for sparklines).

---

### 16. `src/gkr_trading/tui/workers/alpaca_positions.py`

**`AlpacaPositionsWorker.__init__()`:** No params. Creates `UrllibAlpacaHttpClient` from env.

**`poll_once()` -> `tuple[List[LivePosition], Optional[LiveAccountSummary]]`:**
- Hits **`GET /v2/positions`** — parses each into `LivePosition`.
- Hits **`GET /v2/account`** — parses into `LiveAccountSummary`.
- **No market-hours gating.** Polls unconditionally regardless of whether the market is open or closed.

**Poll interval:** `ALPACA_POSITIONS_POLL_INTERVAL = 10.0` seconds.

---

### 17. `src/gkr_trading/tui/workers/db_watcher.py`

**`DBWatcher.__init__(db_path: str)`:** Stores path and `_last_seq: dict[str, int]` watermark.

**`list_sessions()` -> `list[dict]`:** Queries `events` table grouped by `session_id`. Extracts strategy from first event, status/stop_reason from last event.

**`poll_events(session_id)` -> `list[EventSummary]`:** Incremental polling using `_last_seq` watermark. Returns new events since last poll.

Additional methods: `get_strategy_daily_summary()`, `get_sessions_with_dates()`, `get_session_events()`.

---

### 18. `src/gkr_trading/live/traditional/` (26 files)

| File | Status | Description |
|------|--------|-------------|
| **Top-level** | | |
| `__init__.py` | Empty | |
| `buying_power_model.py` | **ABC only** | `BuyingPowerModel` with 3 abstract methods |
| `equity_position_accounting.py` | **Real** | `apply_equity_fill()` with P&L/cost basis |
| `equity_reconciler.py` | **Real** | Compares local vs venue equity positions |
| `market_calendar.py` | **Real** | Simplified (no holiday calendar) |
| `traditional_risk_policy.py` | **Real** | Market hours + position limit checks |
| **alpaca/** | | |
| `__init__.py` | Empty | |
| `alpaca_adapter.py` | **Real** | Full HTTP equity adapter (167 lines) |
| `alpaca_fill_translator.py` | **Real** | Equity fill translation |
| `alpaca_options_adapter.py` | **Partial stub** | Orders work; `get_pending_assignments()` and `get_expiring_today()` return `[]` with TODOs |
| `alpaca_options_fill_translator.py` | **Real** | Options fill + NTA detection |
| `alpaca_options_reconciler.py` | **Real** | Options reconciliation |
| `alpaca_reconciler.py` | **Real** | Equity reconciliation |
| `alpaca_risk_policy.py` | **Real** | PDT day-trade check |
| **options/** | | |
| `__init__.py` | Empty | |
| `assignment_handler.py` | **Real** | Closes options, creates equity position |
| `exercise_handler.py` | **Real** | Same pattern for exercise |
| `expiration_handler.py` | **Real** | Removes options position, realizes P&L |
| `options_adapter_base.py` | **ABC only** | 6 abstract methods |
| `options_domain.py` | **Real** | OCC symbol parsing + chain helper |
| `options_position_accounting.py` | **Real** | All 4 actions handled |
| `options_reconciler.py` | **Real** | Local vs venue comparison |
| `options_risk_policy.py` | **Real** | Config-driven with YAML loading |
| **schwab/** | | |
| `__init__.py` | Empty | |
| `schwab_adapter.py` | **STUB** | All methods raise `NotImplementedError` |
| `schwab_options_adapter.py` | **STUB** | All methods raise `NotImplementedError` |

**Summary:** 17 real, 2 ABCs, 2 stubs (Schwab), 1 partial stub (Alpaca options), 4 empty inits.

---

### 19. `src/gkr_trading/cli/main.py` (577 lines)

**`paper_v2_continuous_cmd` (line 485) parameters:**

| Parameter | Type | Default | CLI Flag |
|-----------|------|---------|----------|
| `db_path` | `str` | Required | `--db-path` |
| `session_id` | `str \| None` | `None` | `--session-id` |
| `strategy` | `Literal["equity","options"]` | `"equity"` | `--strategy` |
| `shadow` | `bool` | `False` | `--shadow` |
| `poll_interval` | `float` | `15.0` | `--poll-interval` |
| `max_cycles` | `int \| None` | `None` | `--max-cycles` |
| `no_websocket` | `bool` | `False` | `--no-websocket` |
| `as_json` | `bool` | `False` | `--json` |

**`tui` command (line 559):** Params: `db_path` (required), `session_id` (optional). Imports `GKRTradingApp` and calls `.run()`.

**All 14 CLI commands:** `init-db`, `ingest-instruments`, `ingest-bars`, `backtest`, `paper`, `paper-dry-run`, `session-inspect`, `replay`, `portfolio-show`, `paper-v2`, `paper-v2-continuous`, `paper-v2-certify`, `tui`, `operator` (sub-app).

---

### 20. `pyproject.toml` (28 lines)

**Core dependencies:**
- `pydantic>=2.5`
- `typer>=0.9`
- `rich>=13.0`

**Optional `[dev]` dependencies:**
- `pytest>=7.4`
- `pytest-cov>=4.1`
- `pyyaml>=6.0`

**Entry point:** `gkr = "gkr_trading.cli.main:app"`

**Build:** `setuptools>=61`, `wheel`

**Python:** `>=3.11`

**Undeclared runtime dependencies:** `textual` (TUI framework), `websocket-client` or `websockets` (WS manager), `pyyaml` (used at runtime in `options_risk_policy.py` with graceful fallback, but listed under dev only).

---

### 21. Environment Variable Documentation

**No `.env.example` file exists** in the repository.

**`README.md`** (35 lines, repo root): Documents setup and 9 of 14 CLI commands. Does NOT document `paper-v2`, `paper-v2-continuous`, `paper-v2-certify`, `tui`, or `operator`. Contains a hardcoded local path (`/Users/rohandhulipalla/GKR INDUSTRIES`).

---

### 22. File Count

```
Total Python files in src/gkr_trading: 149
```

---

## Architecture Summary

```
                          CLI (typer)
                              |
              +---------------+----------------+
              |               |                |
         paper-v2       paper-v2-continuous    tui
              |               |                |
              v               v                v
     PaperSessionRunnerV2   ContinuousSessionRunner   GKRTradingApp (Textual)
              |               |                          |
              |     +---------+---------+       +--------+--------+
              |     |         |         |       |        |        |
              v     v         v         v       v        v        v
         SessionSupervisor  MarketDataFeed  WebSocketMgr  DBWatcher  MarketPoller  PosWorker
              |               |              |              |          |             |
              v               v              v              |          v             v
         ReconciliationSvc   AlpacaHTTP    AlpacaHTTP      |      AlpacaHTTP    AlpacaHTTP
              |               |              |              |      (data API)    (paper API)
              v               v              v              v          |             |
         PositionStore    /v2/stocks/    wss://stream     SQLite   /v2/stocks/   /v2/positions
         EventStore       snapshots      .alpaca.markets            snapshots    /v2/account
         PendingOrderReg                                            /v2/clock
```

**Data flow:**

1. **Market Data Path:** `MarketDataFeed` polls `data.alpaca.markets/v2/stocks/snapshots` every 15s -> creates `MarketDataEnvelope` -> `PaperSessionRunnerV2.process_market_data()` -> Strategy -> Risk Gates -> Broker Adapter -> `paper-api.alpaca.markets/v2/orders`.

2. **WebSocket Path:** `WebSocketManager` connects to `wss://stream.alpaca.markets` -> receives `trade_updates` -> `ContinuousSessionRunner._on_ws_trade_update()` -> `PaperSessionRunnerV2.process_venue_events()` -> Fill/NTA processing -> Position/Event stores.

3. **TUI Path:** Three independent pollers:
   - `DBWatcher` reads SQLite directly (3s interval)
   - `MarketPoller` polls `data.alpaca.markets` + `paper-api.alpaca.markets/v2/clock` (15s interval)
   - `AlpacaPositionsWorker` polls `paper-api.alpaca.markets/v2/positions` + `/v2/account` (10s interval)

4. **Persistence:** All SQLite, single database file. WAL mode with `synchronous=FULL`. Tables: `events`, `equity_positions`, `options_positions`, `pending_orders`.

---

## Key Findings for Redesign

### 1. Exact Line Where Session Terminates on Market Close

**File:** `src/gkr_trading/live/runtime_v2.py`
**Lines 695-709**, specifically **lines 702-705:**

```python
# Market closed
if self._config.stop_after_market_close and self._metadata:
    try:
        if not self._metadata.is_market_open():
            return StopReason.MARKET_CLOSED
    except Exception:
        pass  # Don't halt on metadata errors
```

This calls `AlpacaMarketMetadataProvider.is_market_open()` which hits **`GET /v2/clock`** on `paper-api.alpaca.markets`. The `stop_after_market_close` flag defaults to `True` in `ContinuousSessionConfig` (line 533).

### 2. Whether `/v2/positions` Has Market-Hours Gating

**NO.** The `AlpacaPositionsWorker.poll_once()` in `src/gkr_trading/tui/workers/alpaca_positions.py` hits `GET /v2/positions` and `GET /v2/account` unconditionally with no market-hours check. It polls every 10 seconds regardless of market state. The runtime's `ContinuousSessionRunner` does NOT poll positions — it only polls market data and relies on WebSocket for fills.

### 3. All Environment Variables

| Variable | Required | Default | Used By |
|----------|----------|---------|---------|
| `ALPACA_API_KEY` | **Yes** | — | `AlpacaPaperConfig.from_env()` |
| `ALPACA_SECRET_KEY` | **Yes** | — | `AlpacaPaperConfig.from_env()` |
| `ALPACA_PAPER_BASE_URL` | No | `https://paper-api.alpaca.markets` | `AlpacaPaperConfig.from_env()` |
| `ALPACA_WATCHLIST` | No | `AAPL,SPY,QQQ,TSLA,NVDA,MSFT` | `MarketPoller.__init__()` |

**No `.env.example` file exists.** Environment variables are undocumented outside the code.

### 4. SQLite Schema

**4 tables across 3 modules:**

```sql
-- event_store.py
CREATE TABLE IF NOT EXISTS events (
    session_id   TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    envelope_json TEXT NOT NULL,
    PRIMARY KEY (session_id, seq)
);

-- position_store.py
CREATE TABLE IF NOT EXISTS equity_positions (
    ticker            TEXT NOT NULL,
    venue             TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    signed_qty        INTEGER NOT NULL,
    cost_basis_cents  INTEGER NOT NULL DEFAULT 0,
    realized_pnl_cents INTEGER NOT NULL DEFAULT 0,
    status            TEXT NOT NULL DEFAULT 'open',
    updated_at_ns     INTEGER NOT NULL,
    PRIMARY KEY (ticker, venue, session_id)
);

CREATE TABLE IF NOT EXISTS options_positions (
    occ_symbol                   TEXT NOT NULL,
    venue                        TEXT NOT NULL,
    session_id                   TEXT NOT NULL,
    instrument_ref_json          TEXT NOT NULL,
    long_contracts               INTEGER NOT NULL DEFAULT 0,
    short_contracts              INTEGER NOT NULL DEFAULT 0,
    long_premium_paid_cents      INTEGER NOT NULL DEFAULT 0,
    short_premium_received_cents INTEGER NOT NULL DEFAULT 0,
    realized_pnl_cents           INTEGER NOT NULL DEFAULT 0,
    status                       TEXT NOT NULL DEFAULT 'open',
    has_undefined_risk           INTEGER NOT NULL DEFAULT 0,
    updated_at_ns                INTEGER NOT NULL,
    PRIMARY KEY (occ_symbol, venue, session_id)
);

-- pending_order_registry.py
CREATE TABLE IF NOT EXISTS pending_orders (
    client_order_id   TEXT PRIMARY KEY,
    intent_id         TEXT NOT NULL,
    session_id        TEXT NOT NULL,
    instrument_ref_json TEXT NOT NULL,
    action            TEXT NOT NULL,
    venue             TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending_local',
    venue_order_id    TEXT,
    quantity          INTEGER NOT NULL,
    limit_price_cents INTEGER,
    created_at_ns     INTEGER NOT NULL,
    updated_at_ns     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_po_session ON pending_orders(session_id);
CREATE INDEX IF NOT EXISTS idx_po_status ON pending_orders(status);
```

**PRAGMAs (set in `enforce_wal_mode()`):**
- `journal_mode=WAL`
- `synchronous=FULL`

### 5. Schwab Adapter Status

**Both Schwab adapters are complete stubs:**

- `schwab_adapter.py` (40 lines): All 5 methods raise `NotImplementedError("SchwabAdapter is not yet implemented")`.
- `schwab_options_adapter.py` (30 lines): All 6 methods raise `NotImplementedError("SchwabOptionsAdapter is not yet implemented")`.

Additionally, `alpaca_options_adapter.py` has two stubbed methods:
- `get_pending_assignments()` → returns `[]` with TODO
- `get_expiring_today()` → returns `[]` with TODO

### 6. Current Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLI Entry Points                         │
│  gkr paper-v2-continuous  |  gkr tui  |  gkr paper-v2          │
└─────────┬─────────────────┼───────────┼─────────────────────────┘
          │                 │           │
          ▼                 │           ▼
┌─────────────────────┐     │    ┌──────────────┐
│ ContinuousSession   │     │    │ PaperSession │
│ Runner              │     │    │ RunnerV2     │
│                     │     │    │ (one-shot)   │
│ ┌─────────────────┐ │     │    └──────────────┘
│ │ _run_loop()     │ │     │
│ │  every 15s:     │ │     │
│ │  1. check stop  │ │     │
│ │  2. poll MD     │◄──────────── data.alpaca.markets
│ │  3. process     │ │     │     GET /v2/stocks/snapshots
│ │  4. sleep       │ │     │
│ └────────┬────────┘ │     │
│          │          │     │
│ ┌────────▼────────┐ │     │
│ │ WebSocketMgr    │ │     │
│ │ trade_updates   │◄──────────── wss://stream.alpaca.markets
│ │ (fills, cancel) │ │     │
│ └─────────────────┘ │     │
│                     │     │
│ ┌─────────────────┐ │     │
│ │ _check_stop     │ │     │
│ │  is_market_open │◄──────────── paper-api.alpaca.markets
│ │  (GET /v2/clock)│ │     │     GET /v2/clock (cached 60s)
│ └─────────────────┘ │     │
└──────────┬──────────┘     │
           │                │
           ▼                ▼
┌───────────────────────────────────────┐
│         PaperSessionRunnerV2          │
│                                       │
│  Strategy ──► Risk Gates ──► Broker   │──────► paper-api.alpaca.markets
│                               Adapter │        POST /v2/orders
│                                       │
│  SessionSupervisor                    │
│   ├── startup recon                   │
│   ├── kill switch                     │
│   └── shutdown recon                  │
│                                       │
│  ReconciliationService                │──────► paper-api.alpaca.markets
│   └── local vs venue positions        │        GET /v2/positions
└──────────────┬────────────────────────┘
               │
               ▼
┌───────────────────────────────────────┐
│            SQLite (WAL mode)          │
│                                       │
│  events              (session_id,seq) │
│  equity_positions    (ticker,venue,s) │
│  options_positions   (occ,venue,s)    │
│  pending_orders      (client_order_id)│
└──────────────┬────────────────────────┘
               │
               ▼
┌───────────────────────────────────────┐
│         TUI (GKRTradingApp)           │
│                                       │
│  DBWatcher (3s)                       │
│   └── reads SQLite directly           │
│                                       │
│  MarketPoller (15s)                   │
│   ├── data.alpaca.markets             │
│   │    GET /v2/stocks/snapshots       │
│   └── paper-api.alpaca.markets        │
│        GET /v2/clock                  │
│                                       │
│  AlpacaPositionsWorker (10s)          │
│   └── paper-api.alpaca.markets        │
│        GET /v2/positions (NO hours    │
│        gating — polls 24/7)           │
│        GET /v2/account                │
└───────────────────────────────────────┘
```

### 7. Additional Observations

1. **No third-party HTTP library**: Uses `urllib.request` (stdlib) for all HTTP. No `requests`, `httpx`, or `aiohttp`.
2. **No async anywhere**: Entire codebase is synchronous. WebSocket runs in a daemon thread. TUI workers use `thread=True` background workers.
3. **Undeclared runtime deps**: `textual` and `pyyaml` are used at runtime but not in core dependencies.
4. **Monetary values in cents**: All prices stored as `int` cents throughout (e.g., `cost_basis_cents`, `limit_price_cents`).
5. **Session scoping**: Positions are scoped by `(ticker/occ_symbol, venue, session_id)` — positions do NOT carry across sessions.
6. **Stale filter asymmetry**: Equity stale filter respects `drop_stale` for TUI; options stale filter always drops.
7. **Clock cache**: Market status is cached for 60s, meaning session termination can lag up to 60s after market close.
8. **README is stale**: Documents only 9 of 14 CLI commands, has a hardcoded developer-specific path.
