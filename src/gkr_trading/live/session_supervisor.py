"""SessionSupervisor — lifecycle owner for trading sessions.

Owns: startup reconciliation, run/health loop, reconnect handling,
kill switches, shutdown reconciliation, replay validation hooks,
expiry/assignment safety gates.
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.payloads import (
    ReconciliationCompletedPayload,
    SessionStartedPayload,
    SessionStoppedPayload,
)
from gkr_trading.core.events.types import EventType
from gkr_trading.core.operator_controls import KillSwitchLevel, OperatorAlert
from gkr_trading.core.reconciliation_model import OptionsReconciliationSnapshot
from gkr_trading.live.reconciliation_service import ReconciliationService
from gkr_trading.persistence.event_store import EventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry

logger = logging.getLogger(__name__)


class SessionState(Enum):
    INITIALIZING = "initializing"
    RECONCILING_STARTUP = "reconciling_startup"
    RUNNING = "running"
    SUSPENDED = "suspended"  # websocket disconnect, etc.
    RECONCILING_SHUTDOWN = "reconciling_shutdown"
    HALTED = "halted"
    STOPPED = "stopped"


@dataclass
class SessionContext:
    """Mutable context for the current session."""
    session_id: str
    venue: str
    state: SessionState = SessionState.INITIALIZING
    kill_switch: KillSwitchLevel = KillSwitchLevel.NONE
    alerts: List[OperatorAlert] = field(default_factory=list)
    startup_reconciliation: Optional[OptionsReconciliationSnapshot] = None
    shutdown_reconciliation: Optional[OptionsReconciliationSnapshot] = None
    unknown_orders_reconciled: int = 0


class SessionSupervisor:
    """Lifecycle owner for a trading session."""

    def __init__(
        self,
        event_store: EventStore,
        pending_registry: PendingOrderRegistry,
        reconciliation_service: ReconciliationService,
        session_id: Optional[str] = None,
        venue: str = "unknown",
    ) -> None:
        self._event_store = event_store
        self._pending = pending_registry
        self._recon = reconciliation_service
        self._ctx = SessionContext(
            session_id=session_id or str(uuid.uuid4()),
            venue=venue,
        )

    @property
    def session_id(self) -> str:
        return self._ctx.session_id

    @property
    def state(self) -> SessionState:
        return self._ctx.state

    @property
    def kill_switch(self) -> KillSwitchLevel:
        return self._ctx.kill_switch

    def startup(self) -> bool:
        """Run startup sequence. Returns True if session can proceed.

        1. Mark non-terminal orders as UNKNOWN
        2. Run startup reconciliation
        3. If blocking breaks exist, halt
        4. Emit session_started event
        """
        self._ctx.state = SessionState.RECONCILING_STARTUP

        # Step 1: Mark unknown orders
        unknown_count = self._pending.mark_all_non_terminal_as_unknown(self._ctx.session_id)
        self._ctx.unknown_orders_reconciled = unknown_count
        if unknown_count > 0:
            logger.warning(f"Marked {unknown_count} orders as UNKNOWN for reconciliation")

        # Step 2: Startup reconciliation
        try:
            snapshot = self._recon.reconcile(trigger="startup")
            self._ctx.startup_reconciliation = snapshot

            # Persist reconciliation event
            recon_event = CanonicalEvent(
                event_type=EventType.RECONCILIATION_COMPLETED,
                occurred_at_utc=_now_utc_iso(),
                payload=ReconciliationCompletedPayload(
                    snapshot_id=snapshot.snapshot_id,
                    trigger="startup",
                    status=snapshot.status,
                    break_count=len(snapshot.breaks),
                    blocking_break_count=sum(
                        1 for b in snapshot.breaks if b.severity == "blocking"
                    ),
                ),
            )
            self._event_store.append(self._ctx.session_id, recon_event)
        except Exception as exc:
            logger.error(f"Startup reconciliation failed: {exc}")
            self._ctx.state = SessionState.HALTED
            return False

        # Step 3: Check for blocking breaks
        if snapshot.has_blocking_breaks():
            logger.error(f"Blocking reconciliation breaks detected: {len(snapshot.breaks)}")
            self._ctx.state = SessionState.HALTED
            return False

        # Step 4: Emit session started
        start_event = CanonicalEvent(
            event_type=EventType.SESSION_STARTED,
            occurred_at_utc=_now_utc_iso(),
            payload=SessionStartedPayload(
                session_id=self._ctx.session_id,
                mode="live",
            ),
        )
        self._event_store.append(self._ctx.session_id, start_event)
        self._ctx.state = SessionState.RUNNING
        return True

    def activate_kill_switch(self, level: KillSwitchLevel) -> None:
        """Activate kill switch at specified level."""
        self._ctx.kill_switch = level
        if level == KillSwitchLevel.FULL_HALT:
            self._ctx.state = SessionState.HALTED
            logger.critical("FULL HALT activated")

    def suspend(self, reason: str = "websocket_disconnect") -> None:
        """Suspend session (e.g., websocket disconnect)."""
        if self._ctx.state == SessionState.RUNNING:
            self._ctx.state = SessionState.SUSPENDED
            logger.warning(f"Session suspended: {reason}")

    def resume(self) -> bool:
        """Resume from suspension. Runs reconciliation first."""
        if self._ctx.state != SessionState.SUSPENDED:
            return False
        try:
            snapshot = self._recon.reconcile(trigger="post_reconnect")
            if snapshot.has_blocking_breaks():
                self._ctx.state = SessionState.HALTED
                return False
            self._ctx.state = SessionState.RUNNING
            return True
        except Exception:
            self._ctx.state = SessionState.HALTED
            return False

    def shutdown(self) -> OptionsReconciliationSnapshot:
        """Run shutdown sequence."""
        self._ctx.state = SessionState.RECONCILING_SHUTDOWN

        snapshot = self._recon.reconcile(trigger="shutdown")
        self._ctx.shutdown_reconciliation = snapshot

        # Persist reconciliation event
        recon_event = CanonicalEvent(
            event_type=EventType.RECONCILIATION_COMPLETED,
            occurred_at_utc=_now_utc_iso(),
            payload=ReconciliationCompletedPayload(
                snapshot_id=snapshot.snapshot_id,
                trigger="shutdown",
                status=snapshot.status,
                break_count=len(snapshot.breaks),
                blocking_break_count=sum(
                    1 for b in snapshot.breaks if b.severity == "blocking"
                ),
            ),
        )
        self._event_store.append(self._ctx.session_id, recon_event)

        # Emit session stopped
        stop_event = CanonicalEvent(
            event_type=EventType.SESSION_STOPPED,
            occurred_at_utc=_now_utc_iso(),
            payload=SessionStoppedPayload(
                session_id=self._ctx.session_id,
                reason="normal_shutdown",
            ),
        )
        self._event_store.append(self._ctx.session_id, stop_event)
        self._ctx.state = SessionState.STOPPED
        return snapshot

    def can_submit_orders(self) -> bool:
        """Check if order submission is allowed given current state."""
        if self._ctx.state != SessionState.RUNNING:
            return False
        if self._ctx.kill_switch == KillSwitchLevel.FULL_HALT:
            return False
        return True

    def can_submit_new_orders(self) -> bool:
        """Check if new (opening) orders are allowed. Closing orders
        may still be allowed under CLOSE_ONLY kill switch."""
        if not self.can_submit_orders():
            return False
        if self._ctx.kill_switch == KillSwitchLevel.CLOSE_ONLY:
            return False
        return True


def _now_utc_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
