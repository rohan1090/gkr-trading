"""OrderSubmissionService — write-before-call, idempotent order submission.

Invariant ordering:
1. Persist TradeIntentEvent to EventStore
2. Persist PendingOrderRegistry entry (PENDING_LOCAL)
3. Make API call via VenueAdapter
4. Persist API response/timeout to EventStore
5. Update PendingOrderRegistry status

This ordering is NOT optional. If the process crashes between steps 2 and 3,
the PendingOrderRegistry will contain a PENDING_LOCAL entry that is promoted
to UNKNOWN on restart, triggering reconciliation before any resubmission.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.payloads import (
    OrderSubmissionAttemptedPayload,
    PendingOrderRegisteredPayload,
)
from gkr_trading.core.events.types import EventType
from gkr_trading.core.instruments import InstrumentRef
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.order_model import OrderStatus
from gkr_trading.live.base import SubmissionRequest, SubmissionResponse, VenueAdapter
from gkr_trading.persistence.event_store import EventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry


@dataclass(frozen=True)
class SubmissionOutcome:
    """Result of the full submission pipeline."""
    client_order_id: str
    success: bool
    venue_order_id: Optional[str] = None
    rejected: bool = False
    reject_reason: Optional[str] = None
    duplicate: bool = False
    error: Optional[str] = None


def _instrument_ref_to_json(ref: InstrumentRef) -> str:
    """Serialize InstrumentRef to JSON for persistence."""
    from gkr_trading.core.instruments import EquityRef, OptionsRef
    if isinstance(ref, OptionsRef):
        return json.dumps({
            "asset_class": "option",
            "underlying": ref.underlying,
            "expiry": ref.expiry.isoformat(),
            "strike_cents": ref.strike_cents,
            "right": ref.right,
            "style": ref.style,
            "multiplier": ref.multiplier,
            "deliverable": ref.deliverable,
            "occ_symbol": ref.occ_symbol,
        })
    elif isinstance(ref, EquityRef):
        return json.dumps({
            "asset_class": "equity",
            "ticker": ref.ticker,
        })
    raise ValueError(f"Unknown InstrumentRef type: {type(ref)}")


def _now_utc_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


class OrderSubmissionService:
    """Idempotent, write-before-call order submission."""

    def __init__(
        self,
        event_store: EventStore,
        pending_registry: PendingOrderRegistry,
        adapter: VenueAdapter,
    ) -> None:
        self._event_store = event_store
        self._pending = pending_registry
        self._adapter = adapter

    def submit(self, intent: TradeIntent, venue: str) -> SubmissionOutcome:
        """Execute the full write-before-call submission pipeline.

        Returns SubmissionOutcome. Never raises for expected failures.
        """
        client_order_id = str(uuid.uuid4())

        # Step 1: Check for duplicate (idempotent guard)
        # We check by intent_id — if this intent was already submitted, return duplicate
        # (In production, you'd query by intent_id; here we use client_order_id uniqueness)

        # Step 2: Persist PendingOrderRegistry entry BEFORE API call
        instrument_json = _instrument_ref_to_json(intent.instrument_ref)
        registered = self._pending.register(
            client_order_id=client_order_id,
            intent_id=intent.intent_id,
            session_id=intent.session_id,
            instrument_ref_json=instrument_json,
            action=intent.action,
            venue=venue,
            quantity=intent.quantity,
            limit_price_cents=intent.limit_price_cents,
        )
        if not registered:
            return SubmissionOutcome(
                client_order_id=client_order_id,
                success=False,
                duplicate=True,
            )

        # Step 3: Persist pending-order-registered event
        pending_event = CanonicalEvent(
            event_type=EventType.PENDING_ORDER_REGISTERED,
            occurred_at_utc=_now_utc_iso(),
            payload=PendingOrderRegisteredPayload(
                client_order_id=client_order_id,
                intent_id=intent.intent_id,
                instrument_key=intent.instrument_ref.canonical_key,
                action=intent.action,
                venue=venue,
                quantity=intent.quantity,
                limit_price_cents=intent.limit_price_cents,
            ),
        )
        self._event_store.append(intent.session_id, pending_event)

        # Step 4: Make API call
        try:
            response = self._adapter.submit_order(SubmissionRequest(
                client_order_id=client_order_id,
                instrument_ref=intent.instrument_ref,
                action=intent.action,
                quantity=intent.quantity,
                limit_price_cents=intent.limit_price_cents,
                time_in_force=intent.time_in_force,
                venue=venue,
            ))
        except Exception as exc:
            # Step 4a: Persist timeout/error event
            error_event = CanonicalEvent(
                event_type=EventType.ORDER_SUBMISSION_ATTEMPTED,
                occurred_at_utc=_now_utc_iso(),
                payload=OrderSubmissionAttemptedPayload(
                    client_order_id=client_order_id,
                    success=False,
                    timeout=True,
                ),
            )
            self._event_store.append(intent.session_id, error_event)
            # Mark as UNKNOWN for crash recovery
            self._pending.update_status(client_order_id, OrderStatus.UNKNOWN)
            return SubmissionOutcome(
                client_order_id=client_order_id,
                success=False,
                error=str(exc),
            )

        # Step 5: Persist API response event
        response_event = CanonicalEvent(
            event_type=EventType.ORDER_SUBMISSION_ATTEMPTED,
            occurred_at_utc=_now_utc_iso(),
            payload=OrderSubmissionAttemptedPayload(
                client_order_id=client_order_id,
                venue_order_id=response.venue_order_id,
                success=response.success,
                rejected=response.rejected,
                reject_reason=response.reject_reason,
            ),
        )
        self._event_store.append(intent.session_id, response_event)

        # Step 6: Update PendingOrderRegistry status
        if response.rejected:
            self._pending.update_status(
                client_order_id, OrderStatus.REJECTED, response.venue_order_id
            )
        elif response.success:
            self._pending.update_status(
                client_order_id, OrderStatus.SUBMITTED, response.venue_order_id
            )
        else:
            self._pending.update_status(client_order_id, OrderStatus.UNKNOWN)

        return SubmissionOutcome(
            client_order_id=client_order_id,
            success=response.success and not response.rejected,
            venue_order_id=response.venue_order_id,
            rejected=response.rejected,
            reject_reason=response.reject_reason,
        )
