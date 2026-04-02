# Adversarial validation matrix (V1)

Post–correctness-hardening scenarios (fill_id, lifecycle, strict replay): see **`CORRECTNESS_HARDENING_MATRIX.md`**.

Columns: scenario | why it matters | expected invariant | modules | coverage | action

## Event / replay

| # | Scenario | Why | Invariant | Modules | Coverage | Action |
|---|----------|-----|-----------|---------|----------|--------|
| 1 | duplicate `fill_received` **same `fill_id`** | log corruption / double-apply | idempotent — second no-op | `portfolio/transitions`, `replay` | `test_duplicate_fill_same_fill_id_is_idempotent` | **hardened** |
| 1b | duplicate fills **different `fill_id`** | two broker execs | both apply | `transitions` | `test_two_distinct_fills_same_order_increase_position` | — |
| 1c | same hash partials **without** `dedupe_salt` | collision | **only first applies** | `schemas/ids` | `test_two_partial_fills_same_deterministic_hash_without_salt_collide` | operator must salt / use exec id |
| 2 | duplicate `order_acknowledged` | broker retries | no cash/position mutation | `portfolio/transitions` | adversarial | ack is no-op ✓ |
| 3 | duplicate `session_started` / `session_stopped` | operator mistakes | replay portfolio unchanged (skipped) | `replay/engine` | adversarial | — |
| 4 | replay same session twice | idempotence | bitwise-identical `PortfolioState` fields | `replay/engine` | adversarial | — |
| 5 | strip `portfolio_updated` | audit vs causal | replay final state unchanged | `replay/engine`, `portfolio` | adversarial | validates law 9 |
| 6 | inject extra audit-only events | noise | replay truth unchanged | `replay/engine` | adversarial | — |
| 7 | reorder ack vs fill | ordering sensitivity | ack no-op → order invariant | `portfolio/transitions` | adversarial | — |
| 8 | fill without prior ack | partial streams | **V1 applies fill to P&L/position** | `portfolio/transitions` | adversarial | document gap |
| 9 | unknown `event_type` | forward compat | load fails loudly | `events/serde`, `envelope` | adversarial | pydantic rejects |
| 10 | corrupted payload | disk corruption | load fails loudly | `serde` | adversarial | ValidationError |

## Portfolio / order lifecycle

| # | Scenario | Why | Invariant | Modules | Coverage | Action |
|---|----------|-----|-----------|---------|----------|--------|
| 11 | partial then final fill | realism | qty/cash/open_orders consistent | `portfolio/transitions` | adversarial | — |
| 12 | partial fill then cancel | realism | **V1: no cancel event** | — | skip / REMEDIATION | phase 2 |
| 13 | reject after intent | risk wall | no position/cash change | `portfolio/transitions` | adversarial | — |
| 14 | submitted never ack | open order | remains in `open_orders` | `portfolio` | adversarial | — |
| 15 | ack never filled | ghost working order | open_orders unchanged by ack | `portfolio` | adversarial | — |
| 16 | duplicate open order guard | risk | second intent rejected | `risk/engine` | adversarial | — |
| 17 | sell > long qty | fat-finger | **V1 opens short for remainder** | `portfolio/transitions` | adversarial | document production risk |
| 18 | multiple fills avg cost | accumulation | weighted avg | `portfolio/transitions` | adversarial | — |
| 19 | buy then sell realized | P&L | realized matches close-out | `portfolio/transitions` | unit+adversarial | — |
| 20 | mark update | MTM | unrealized moves; realized flat | `portfolio/transitions` | adversarial | — |

## Risk

| # | Scenario | Why | Invariant | Modules | Coverage | Action |
|---|----------|-----|-----------|---------|----------|--------|
| 21–24 | limits / kill / session | compliance | reject with codes | `risk/engine` | unit+adversarial | — |
| 25 | repeated intent same tick | spam | second blocked if open order | `risk/engine` | adversarial | — |

## Convergence

| # | Scenario | Why | Invariant | Modules | Coverage | Action |
|---|----------|-----|-----------|---------|----------|--------|
| 26–29 | backtest vs paper | law 7 | same economics + fill contract | `backtest/*`, `live/*` | integration+adversarial | — |

## Data / identity

| # | Scenario | Why | Invariant | Modules | Coverage | Action |
|---|----------|-----|-----------|---------|----------|--------|
| 30 | symbol leakage | law 4 | bars keyed by `instrument_id` in API rows | `access_api` | adversarial | — |
| 31 | universe membership | correctness | members ⊆ registry | `universe_registry` | adversarial | — |
| 32 | futures/options tables | scaffolding | query uses whitelisted table | `market_store`, `access_api` | adversarial | — |
| 33 | derived not required | law 5,10 | replay/backtest w/o derived | `derived_views` | adversarial | NullDerivedViews |

## CLI / operator

| # | Scenario | Why | Invariant | Modules | Coverage | Action |
|---|----------|-----|-----------|---------|----------|--------|
| 34–35 | missing session | ops | empty load → initial-ish replay | `event_store`, `cli` | adversarial | — |
| 36 | portfolio-show no fills | ops | cash = starting | `cli`/`replay` | adversarial | — |
| 37 | empty DB bars | honesty | no crash | `orchestrator` | adversarial | — |
| 38 | seed path | docs | deterministic demo IDs | `cli/seed` | adversarial | — |
