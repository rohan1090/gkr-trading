# V1 gaps surfaced by adversarial validation

These are **intentionally not** “fixed” as features in the audit pass; they are documented production risks.

1. **No `order_cancelled` / partial-lifecycle events** — open order state cannot be corrected from the log alone when a remainder is cancelled at the broker. Remediation: add canonical cancel/replace events and reducer rules in a later phase.

2. **Duplicate `fill_received` is not idempotent** — a corrupted or replayed log can double-apply inventory. Remediation: broker fill IDs + idempotency keys in events and reducer, or external reconciliation.

3. **Oversized sell closes long then opens short** — `portfolio/transitions._apply_fill_to_instrument` follows a single-account netting model without a “max sell = position” guard. Remediation: optional intent-time or fill-time guard for cash accounts; keep separate margin model for shorts.

4. **`ORDER_SUBMITTED` idempotency** — same `order_id` while already open is ignored (small fix applied) to avoid accidental overwrite of `remaining_qty` on duplicate submits; a **second** submit after the order left `open_orders` can still re-open the same logical order id — full lifecycle needs state machine.

5. **Corrupt rows in SQLite** — `load_session` raises; operators must handle exceptions in CLI (Typer stack trace). Remediation: CLI try/except with exit code 1 + message (small UX change, not done in audit).
