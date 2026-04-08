"""Risk approval gate — abstract interface for risk policy chains.

RiskApprovalGate is the ABC that all risk policies implement.
The core module does NOT contain venue-specific or asset-class-specific logic.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RiskDecision:
    """Outcome of a risk evaluation."""
    approved: bool
    order_id: Optional[str] = None
    reason_code: Optional[str] = None
    reason_detail: Optional[str] = None


class RiskApprovalGate(ABC):
    """Abstract risk policy. Implementations in live/traditional/ or live/prediction/."""

    @abstractmethod
    def evaluate(
        self,
        intent: object,  # TradeIntent (old or new)
        context: object,  # SessionContext or PortfolioState
    ) -> RiskDecision:
        """Evaluate an intent against risk policy. Returns RiskDecision."""
        ...
