"""Expiration handler — processes ExpirationEvent.

Expiration is NOT a fill. It removes the options position
with no cash flow and no equity position change.
"""
from __future__ import annotations

from gkr_trading.core.options_lifecycle import ExpirationEvent
from gkr_trading.core.position_model import OptionsContractRecord


def process_expiration(
    event: ExpirationEvent,
    current: OptionsContractRecord | None,
) -> OptionsContractRecord:
    """Process an expiration event. Returns updated (closed) options record.

    Expiration removes the position. No cash flow.
    Premium paid on longs is a sunk cost.
    Premium received on shorts is now kept.
    """
    ref = event.instrument_ref

    if current is not None:
        # For longs: premium paid is realized loss
        long_loss = 0
        if current.long_contracts > 0:
            long_loss = -current.long_premium_paid_cents

        # For shorts: premium received is realized gain
        short_gain = 0
        if current.short_contracts > 0:
            short_gain = current.short_premium_received_cents

        return OptionsContractRecord(
            instrument_ref=ref,
            venue=event.venue,
            long_contracts=0,
            short_contracts=0,
            long_premium_paid_cents=current.long_premium_paid_cents,
            short_premium_received_cents=current.short_premium_received_cents,
            realized_pnl_cents=current.realized_pnl_cents + long_loss + short_gain,
            status="expired",
            has_undefined_risk=False,
        )

    return OptionsContractRecord(
        instrument_ref=ref,
        venue=event.venue,
        long_contracts=0,
        short_contracts=0,
        long_premium_paid_cents=0,
        short_premium_received_cents=0,
        realized_pnl_cents=0,
        status="expired",
        has_undefined_risk=False,
    )
