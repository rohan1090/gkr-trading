# Paper / Live Orchestrator Specification (V1)

## 1. Purpose

This document defines how asynchronous broker reality is serialized into the GKR V1 Python runtime: a single-threaded, bar-stepped control loop that appends canonical events to the local SQLite event store. It constrains implementation so that **append order (`seq`) remains the only replay ordering truth**, the broker never becomes portfolio source of truth, and strategy code stays free of broker callbacks.

## 2. Current Runtime Audit (Summary)

- **Backtest** (`backtest/orchestrator.py`): linear `for bar in bars`; execution events are produced synchronously inside the loop.
- **Paper** (`live/runtime.py`): same bar driver; historically mocked submit → immediate ACK → synthetic fill in one iteration.
- **Persistence** (`persistence/event_store.py`): `seq = MAX(seq)+1` per append; `load_session` orders by `seq`.
- **Replay** (`core/replay/engine.py`): folds portfolio by iterating events in `seq` order; payload timestamps are not used for ordering.

**Tension:** Real brokers deliver ACKs, fills, cancels, and rejects asynchronously. V1 resolves this without websockets or distributed queues by **polling only at explicit sync fences** on the same thread that appends and folds state.

## 3. Chosen V1 Orchestrator Model

V1 uses **bar-stepped polling** with a **single consumer thread** (the session loop). All broker I/O runs inside named sync phases. The `BrokerAdapter` boundary returns **normalized facts** (`BrokerFillFact`, `BrokerOrderRejectedFact`, `BrokerOrderCancelledFact`); the runtime translates facts to canonical events, appends, and folds—never the adapter mutating `PortfolioState`.

**Real Alpaca paper client:** `live/alpaca_paper_adapter.py` implements `BrokerAdapter` against Alpaca **paper** REST only (submit + **paginated** poll). SQLite table `broker_reconciliation_state` stores adapter progress (emitted ids + tracked orders); `run_paper_session` persists after each sync fence and rehydrates open orders from the event log on startup. Submit uncertainty uses `client_order_id` search (paginated `/v2/orders`), never blind resubmit — see `docs/ALPACA_PAPER_ADAPTER.md`. Historical market data remains on the local Schwab-fed store; the adapter does not ingest Alpaca bars as canonical research data.

## 4. Sync Fences

### 4.1 `broker_sync_startup`

Runs **once** after `SESSION_STARTED` and **before** the first bar’s `MARKET_DATA_RECEIVED`. Drains broker facts that must be reflected before any strategy logic (e.g. recovery after restart). Uses the current `BrokerReconciliationCursor` (see §7).

### 4.2 `broker_sync_pre_bar`

Runs **before** `market_bar` append and **before** `strategy.on_bar` for that bar. Ensures the in-memory portfolio matches broker-acknowledged reality through the end of the prior bar (or startup), so the strategy observes a **stable snapshot**.

### 4.3 `broker_sync_post_submit`

Runs **after** `ORDER_SUBMITTED`, broker `submit`, and optional `ORDER_ACKNOWLEDGED`, **before** continuing the same bar’s execution path. Captures immediate synchronous or near-synchronous broker outcomes (e.g. REST rejection, first partial fills).

### 4.4 `broker_sync_post_bar`

Runs **after** the bar’s strategy and order path (or after a bar with no intent). Captures fills and lifecycle updates that appear slightly later while still serializing them **before** the next bar’s `broker_sync_pre_bar`.

### 4.5 Deterministic ordering within one sync batch

Within a **single** `poll_broker_facts` result, facts are sorted **before** append as follows:

1. **Reject** facts (`BrokerOrderRejectedFact`)
2. **Cancel** facts (`BrokerOrderCancelledFact`)
3. **Fill** facts (`BrokerFillFact`)

Within each class, sort by `(occurred_at_utc, tie_breaker)` where `tie_breaker` is `broker_execution_id` for fills and `str(client_order_id)` for reject/cancel.

**Law:** Never reorder across already committed `seq` values; never rewrite history.

## 5. Event Ordering Law

| Concept | Rule |
|--------|------|
| **Replay order** | SQLite `seq` ascending only. |
| **Broker timestamps** | Carried on payloads as `occurred_at_utc` / `fill_ts_utc` for audit; **not** used to sort replay. |
| **Late facts** | Appended with new `seq`; may have `occurred_at_utc` earlier than prior events—this is honest delivery delay. |
| **Idempotency** | Fills dedupe by `broker_execution_id` → canonical `FillId` (`exec:{id}`); runtime skips duplicate execution ids within a session before append. |

## 6. Strategy / Broker Isolation Rules

- Strategy runs **only** inside `strategy.on_bar` after `broker_sync_pre_bar` and the current bar’s `market_bar` fold.
- **No** broker callbacks into strategy; **no** websocket handlers mutating portfolio on background threads in V1.
- Broker facts are ingested **only** inside sync fences on the session thread.
- Open orders and positions visible to strategy are those implied by **folded canonical state**, not live broker JSON.

## 7. Restart / Recovery Rules

- A **`BrokerReconciliationCursor`** (opaque token interpreted by the adapter) advances as polls complete. Phase 1 may keep it **in-memory** only; callers may pass a mutable `PaperSessionBrokerHooks` to observe the final cursor for persistence in a later phase.
- **Startup reconciliation** must run before the first post-restart strategy step (`broker_sync_startup`).
- **Duplicate recovery:** adapter may return facts already in the log; runtime drops fills whose `broker_execution_id` was already appended in the session (`seen_broker_execution_ids`).
- **Anomalies:** permissive replay continues to surface lifecycle violations via `ReplayResult.anomalies`; strict mode remains available for audit gates.

## 8. V1 Implement Now vs Later

| Now (Phase 1) | Later |
|---------------|--------|
| Extended `BrokerAdapter` + normalized facts + poll API | Real Alpaca REST adapter |
| Bar-stepped sync fences in `run_paper_session` | Durable cursor persisted alongside session |
| Instrument master → executable symbol resolver (Alpaca equity policy) | Options/futures execution mapping |
| Deterministic within-batch ordering + fill dedupe | WebSocket **buffer** + same-thread `pump()` only |
| Single-threaded append/fold | Multi-strategy / distributed infra (out of scope) |

## 9. Key Invariants

1. Append-only event log is the source of truth.
2. Broker adapter **never** mutates `PortfolioState` directly.
3. Every broker-originated state change appears as a canonical event appended through `SessionManager`.
4. Replay uses `seq` order exclusively.
5. V1 orchestration is **polling at fences**, not concurrent broker-driven mutation.
6. Websocket ingestion, if added later, must **buffer** and drain only from sync fences on the session thread.

## 10. Operator reporting (session end)

After a successful `run_paper_session`, callers receive `PaperSessionRunResult` (`state`, `report`). The **report describes** folded state, recovery telemetry, event counts, and a non-strict replay pass for reconciliation anomalies — it does **not** replace the append-only log.

- **CLI:** `gkr paper` (default JSON report), `gkr paper --quiet`, `gkr paper-dry-run` (mock broker, no network; `adapter_mode=dry_run`).
- **Historical inspection:** `gkr session-inspect` adds `replay_anomaly_types` (code histogram); live-only fields such as submit-uncertainty resolution text are **not** persisted outside the canonical event stream.

Implementation: `live/paper_session_report.py` (`PaperSessionOperatorReport`, `build_paper_session_operator_report`).

## 11. Operator reporting (partial failure)

On exceptions inside the paper session loop, the runtime raises `PaperSessionRunFailed` with an attached `PaperSessionFailureReport`.\n+\n+Properties:\n+\n+- **Honest**: fields are derived from persisted events + in-memory recovery counters at the time of failure.\n+- **Non-mutating**: the report does not change canonical events or replay semantics.\n+- **Explicit failure**: CLI exits non-zero; the exception class/message is included (`failure_type`, `failure_message`) along with `failure_phase`.\n+\n+CLI prints the failure report JSON by default for `gkr paper` and `gkr paper-dry-run`. `--quiet` prints a bounded one-line failure summary and still exits non-zero.
