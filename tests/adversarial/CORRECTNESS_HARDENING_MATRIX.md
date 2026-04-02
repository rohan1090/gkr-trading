# Adversarial matrix — correctness hardening (V1)

Schema: **scenario** | **expected behavior** | **invariant** | **modules** | **status**

## Fill idempotency

| # | Scenario | Expected | Invariant | Modules | Status |
|---|----------|----------|-----------|---------|--------|
| 1 | Same fill appended twice (identical payload / fill_id) | Second application no-op | `applied_fill_ids`; position unchanged | `portfolio/transitions._apply_fill_received`, `replay/engine` | **PASS** (`test_correctness_layer_adversarial`, `test_replay_events_adversarial`) |
| 2 | Replay same session twice | Bitwise identical `PortfolioState` | Idempotent fold | `replay/engine`, `portfolio/transitions` | **PASS** |
| 3 | Two partials, same (order, ts, qty, price, fees), no salt | **Second fill dropped** (colliding fill_id) | Callers must set distinct `fill_id` / `broker_execution_id` / `dedupe_salt` | `schemas/ids.deterministic_fill_id_v1`, `payloads.FillReceivedPayload` | **DOCUMENTED FAIL** (`test_two_partial_fills_same_deterministic_hash_without_salt_collide`) |
| 4 | Two partials with `dedupe_salt` | Both apply | Two canonical executions | `events/builders.fill_received` | **PASS** (`test_two_distinct_fills_same_order_increase_position`) |
| 5 | `fill_id` + conflicting `broker_execution_id` in JSON | **`fill_id` wins**; no cross-check | Adapter must not emit contradictory pair | `events/payloads._normalize_fill_id` | **PASS** (`test_broker_execution_id_ignored_when_fill_id_explicitly_set`) — *implicit, not reconciled* |

## Order lifecycle

| # | Scenario | Expected | Invariant | Modules | Status |
|---|----------|----------|-----------|---------|--------|
| 6 | Ack before submit | Ack no-op; later submit+fill OK | No orphan-ack anomaly (silent) | `portfolio/transitions` ORDER_ACK | **PASS** (`test_ack_before_submit_is_silent_noop_then_normal_flow`) |
| 7 | Fill before submit (orphan) permissive | PnL applies; optional `ORPHAN_FILL` if `anomalies=` | Not strict by default | `_apply_fill_received` | **PASS** (`test_fill_without_prior_submit_still_mutates_position`, `test_orphan_fill_is_silent_without_anomalies_sink`) |
| 8 | Fill before submit strict | Raises `StrictReplayError` | Honest rejection | `replay_portfolio_state(strict=True)` | **PASS** (`test_correctness_hardening` + adversarial strict) |
| 9 | Fill before ack (valid submit) | Fill applies; ack may follow as no-op | `PARTIALLY_FILLED` / `FILLED` | `transitions` | **PASS** (`test_reorder_ack_before_fill`) |
| 10 | Partial then final | `remaining_qty` + lifecycle + terminal `FILLED` | Open order map consistent | `transitions` | **PASS** (portfolio + unit hardening) |
| 11 | Duplicate submit while open | Ignored | No reset of `remaining_qty` | `ORDER_SUBMITTED` branch | **PASS** (`test_duplicate_order_submitted_idempotent_preserves_partial`, `test_duplicate_submit_while_open_still_idempotent`) |
| 12 | Submit after terminal `FILLED` | Permissive: skip + anomaly; Strict: raise | No re-open terminal order | `violations_for_order_submitted` | **PASS** (unit hardening strict) |
| 13 | Second fill after `FILLED` (new fill_id) | No economics; Strict raises `FILL_AFTER_FILLED` | No silent double inventory | `_apply_fill_received` | **PASS** (`test_strict_replay_rejects_second_fill_after_order_filled`) |
| 14 | Cancel / broker reject events | **Not in schema** | Must be explicit gap | — | **UNSUPPORTED** (REMEDIATION) |

## Replay / serde

| # | Scenario | Expected | Invariant | Modules | Status |
|---|----------|----------|-----------|---------|--------|
| 15 | Strip `portfolio_updated` | Identical replay truth | Causal subset | `replay/engine._SKIP_REPLAY` | **PASS** |
| 16 | Duplicate ack | Harmless | no-op | `ORDER_ACK` | **PASS** |
| 17 | Duplicate fill (same id) | Idempotent | `applied_fill_ids` | `transitions` | **PASS** |
| 18 | Malformed payload | Load fails | No partial state from bad row | `persistence/event_store`, `serde`, pydantic | **PASS** (corrupt JSON / bad UUID); **Caveat**: legacy fill coerce path can raise `decimal.InvalidOperation` before `ValidationError` if `fill_id` omitted and `fill_price` garbage |
| 19 | Unknown event type / schema version | Fail at load | Loud failure | `serde.loads_event`, `EventEnvelope` | **PASS** |
| 20 | Strict replay mode | Invalid streams raise | No silent corruption | `replay_portfolio_state(strict=True)` | **PASS** (`test_correctness_hardening`, adversarial strict fill-after-filled) |

## Convergence

| # | Scenario | Expected | Invariant | Modules | Status |
|---|----------|----------|-----------|---------|--------|
| 21 | Backtest vs paper economics | Same cash/positions/realized (for shared strategy path) | Shared event reducers | `backtest/orchestrator`, `live/runtime` | **PASS** (`test_convergence_adversarial`) |
| 22 | Happy-path after hardening | No regression | Builders attach `fill_id` | `execution_simulator`, `fill_handler` | **PASS** (integration + adversarial) |

---

## Audit pointers (implementation)

- **Fill idempotency:** `transitions._apply_fill_received` lines 197–198 (`fid_str in state.applied_fill_ids`).
- **Lifecycle:** `OrderLifecycleState` on `OpenOrder`; `order_lifecycle` map; `ORDER_SUBMITTED` / `ORDER_ACKNOWLEDGED` / fill branches in `apply_canonical_event`.
- **Validation:** `events/validate.py` (`violations_for_order_submitted`, `violations_fill_against_open_order`); fill orphan / terminal handled in `_apply_fill_received`.
- **Replay:** `replay_portfolio_state(..., strict=, anomalies=)`; observational skip set `_SKIP_REPLAY`.
- **Implicit assumptions:** (1) `seq` total order is trusted — no out-of-order repair; (2) permissive orphan fills **silent** unless `anomalies=` provided; (3) deterministic fill hash collision without salt is a **real foot-gun**; (4) forward path (`orchestrator` / `runtime`) still uses `strict=False` — production gating is replay/analyzer concern unless wired.
