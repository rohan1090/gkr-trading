# Alpaca paper adapter (V1)

## What it is

`AlpacaPaperAdapter` (`src/gkr_trading/live/alpaca_paper_adapter.py`) is a **thin** implementation of the existing `BrokerAdapter` protocol:

- **POST** `/v2/orders` on `submit()` after the runtime has already appended `ORDER_SUBMITTED`.
- **GET** `/v2/orders` and **GET** `/v2/account/activities` on `poll_broker_facts()` for each orchestrator sync fence (`STARTUP` / `PRE_BAR` / `POST_SUBMIT` / `POST_BAR`).
- **Paginated** polling (bounded, up to 50 pages × 100 rows per endpoint per poll) so broker facts are not silently dropped behind a single-page cap.
- **Durable adapter state** in SQLite table `broker_reconciliation_state` (emitted fill activity ids, lifecycle emit flags, tracked `SubmitRequest` + Alpaca order id rows), saved after every sync phase when using this adapter via `run_paper_session`.
- **Restart rehydration**: open orders + broker ids are also rebuilt from the canonical event log (`rehydrate_tracked_orders_from_events`) and merged into the adapter before `STARTUP` sync.

Normalized outputs remain: `SubmissionResult`, `BrokerFillFact`, `BrokerOrderCancelledFact`, `BrokerOrderRejectedFact`. **No** `PortfolioState` mutation in the adapter, **no** raw Alpaca JSON in core, **no** websockets, **no** live URL.

## What it is not

- Not a market-data or bar-history pipeline. Alpaca MD is **out of scope**; canonical historical data stays in the local store (Schwab-fed).
- Not a guarantee of perfect submit visibility: Alpaca exposes no “GET order by `client_order_id`”; uncertainty recovery **paginates** `/v2/orders` (max 50×100). If an order exists but is not returned in that window, recovery may falsely report “not found” — **safest behavior is to stop and let an operator reconcile** (`AlpacaSubmitUnresolvedError`).
- Not unbounded history replay: pagination caps remain a **V1** tradeoff.

## Configuration

| Variable | Required | Default |
|----------|----------|---------|
| `ALPACA_API_KEY` | yes | — |
| `ALPACA_SECRET_KEY` | yes | — |
| `ALPACA_PAPER_BASE_URL` | no | `https://paper-api.alpaca.markets` |

Load via `AlpacaPaperConfig.from_env()`. Missing keys raise `AlpacaConfigError`.

## Runtime wiring

```python
from gkr_trading.live.alpaca_config import AlpacaPaperConfig
from gkr_trading.live.alpaca_paper_adapter import AlpacaPaperAdapter
from gkr_trading.live.broker_symbol import make_alpaca_equity_symbol_resolver
from gkr_trading.data.instrument_master.repository import InstrumentRepository

cfg = AlpacaPaperConfig.from_env()
repo = InstrumentRepository(conn)
broker = AlpacaPaperAdapter(cfg)
run_paper_session(
    ...,
    broker=broker,
    symbol_resolver=make_alpaca_equity_symbol_resolver(repo),
    broker_session=my_ctx,  # optional: read my_ctx.recovery after run
    resume_existing_session=False,  # True: skip second SESSION_STARTED if log already has one
)
```

`SubmitRequest.executable_broker_symbol` must be set (via `symbol_resolver`) for submits. Whole-share equity qty only.

## Submit behavior

- **4xx** (not 429): `SubmissionResult(rejected=True)`; order not tracked for poll (avoids duplicate reject).
- **429 / 5xx / timeout / OSError**: `AlpacaSubmitUncertaintyError`.
- **Runtime recovery** (no resubmit): on that exception, `AlpacaPaperAdapter.resolve_submit_uncertainty(submit_req)` paginates `/v2/orders` searching for `client_order_id`.
  - **Found** (accepted/open/etc.): registers tracking; session continues with `SubmissionResult` from recovery.
  - **Found rejected**: returns structured reject result.
  - **Not found**: `AlpacaSubmitUnresolvedError` — session stops; `PaperBrokerSessionContext.recovery` lists the anomaly.

## Polling & persistence

- Each poll walks **all pages** (until short page / empty / max pages) for orders and FILL activities.
- After each `run_broker_sync_phase`, the runtime persists `export_persisted_payload()` to `broker_reconciliation_state`.
- Canonical log still wins for fill dedupe: `seen_broker_execution_ids` seeds adapter emitted-fill set on startup.

## Operator visibility (`PaperSessionRecoveryReport`)

`PaperBrokerSessionContext.recovery` (see `broker_sync.py`) accumulates:

- `used_persisted_broker_state`
- `rehydration_anomalies` (e.g. open order without ACK, unresolved submit)
- `startup_broker_facts_seen` (raw fact count from `STARTUP` poll batch)
- `cumulative_pagination_order_pages` / `cumulative_pagination_activity_pages`
- `uncertainty_resolution_log` (per client_order_id resolve outcome)

### End-of-session operator report

`run_paper_session` returns `PaperSessionRunResult` with `state` and `report` (`PaperSessionOperatorReport` in `live/paper_session_report.py`). The report merges:

- **Recovery:** persisted broker state loaded, startup fact counts, pagination totals, rehydration anomalies, submit-uncertainty log and resolved/unresolved flags (derived from log + anomalies).
- **Canonical log counts:** orders submitted, acks, fills, cancels, rejects.
- **Replay (non-strict):** anomaly count, top anomaly codes, `replay_consistency_hint` (`ok` vs `anomalies_N`).
- **Session semantics:** `resumed_session` / `started_fresh`, `bars_processed`, `adapter_mode` (`alpaca` | `mock` | `dry_run`).

CLI `gkr paper` prints this report by default (JSON). Use `--quiet` for the legacy one-line summary; `--json` for report-only JSON. **`gkr paper-dry-run`** runs the same orchestration with a mock broker, `dry_run` report mode, no Alpaca credentials and no network — useful to verify fences and reporting, not live Alpaca behavior.

If the session aborts (e.g. `AlpacaSubmitUnresolvedError`), the runtime raises `PaperSessionRunFailed` carrying a **partial** `PaperSessionFailureReport` built from:\n+\n+- canonical events persisted so far\n+- `PaperBrokerSessionContext.recovery` fields accumulated so far\n+- best-effort non-strict replay on persisted events\n+\n+The failure remains explicit (exception + non-zero CLI exit). Unknown fields remain `null`/empty rather than guessed.

## Resume semantics (pragmatic V1)

`resume_existing_session=True`: if the event log **already** contains `SESSION_STARTED` for this `session_id`, the runtime **does not** append another `SESSION_STARTED`. It does not rewrite or merge duplicate metadata beyond that — full session editor tooling is out of scope.

## Mock vs Alpaca

| | `MockBrokerAdapter` | `AlpacaPaperAdapter` |
|---|---------------------|------------------------|
| Use | tests, local sim | paper account only |
| Network | none | Alpaca REST |
| Cursor / reconcile JSON | no | yes |

## Before controlled unattended paper

- Monitor `AlpacaSubmitUnresolvedError` and `recovery.rehydration_anomalies`.
- Ensure `broker_reconciliation_state` lives on the same SQLite file as `events` (shared `conn`).
- Validate symbol map for every instrument.
- Understand pagination / uncertainty **limits** above; very high order volume may require a future cursor keyed to Alpaca `until` / time window (not implemented).
