"""Operator controls — kill switches, alerts, manual approval gates."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional


class KillSwitchLevel(Enum):
    """Graduated kill switch levels."""
    NONE = "none"               # normal operation
    CLOSE_ONLY = "close_only"   # only closing orders allowed
    FULL_HALT = "full_halt"     # no orders at all


@dataclass(frozen=True)
class OperatorAlert:
    """Alert raised to operator — logged as domain event."""
    alert_id: str
    session_id: str
    severity: Literal["info", "warning", "critical"]
    category: str  # e.g. "assignment", "reconciliation_break", "undefined_risk"
    message: str
    timestamp_ns: int
    requires_ack: bool = False
    acknowledged: bool = False


@dataclass(frozen=True)
class OperatorCommand:
    """Operator-issued command — persisted before execution."""
    command_id: str
    session_id: str
    command_type: str  # "kill_switch", "halt", "resume", "approve", "reject"
    parameters: Optional[str] = None  # JSON-serialized parameters
    timestamp_ns: int = 0
    operator_id: str = "cli"


class ManualApprovalGate:
    """Gate for orders that require operator approval before submission.

    Stub — full implementation in Phase 3 CLI.
    """

    def requires_approval(self, intent: object) -> bool:
        """Check if an intent requires manual approval."""
        return False

    def request_approval(self, intent: object) -> str:
        """Request approval for an intent. Returns approval_id."""
        raise NotImplementedError("ManualApprovalGate not yet implemented")
