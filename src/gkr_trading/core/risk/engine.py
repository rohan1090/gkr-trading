from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING

from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.portfolio.models import PortfolioState
from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import OrderId

if TYPE_CHECKING:
    from gkr_trading.core.schemas.clock import Clock


@dataclass(frozen=True)
class RiskLimits:
    max_position_abs: Decimal
    max_notional_per_trade: Decimal
    session_start_utc: time
    session_end_utc: time
    kill_switch: bool = False


@dataclass(frozen=True)
class RiskResult:
    approved: bool
    order_id: OrderId | None = None
    reason_code: str | None = None
    reason_detail: str | None = None


def _projected_position_abs(state: PortfolioState, intent: TradeIntent) -> Decimal:
    iid = str(intent.instrument_id)
    q = state.positions.get(iid, Decimal("0"))
    delta = intent.quantity if intent.side == OrderSide.BUY else -intent.quantity
    return abs(q + delta)


def _notional(intent: TradeIntent, mark: Decimal | None) -> Decimal:
    if intent.order_type == OrderType.LIMIT and intent.limit_price is not None:
        return intent.quantity * intent.limit_price
    if mark is not None:
        return intent.quantity * mark
    return Decimal("0")


def _duplicate_open_order(state: PortfolioState, intent: TradeIntent) -> bool:
    for oo in state.open_orders.values():
        if oo.instrument_id == intent.instrument_id and oo.side == intent.side:
            return True
    return False


def _in_session(now: datetime, limits: RiskLimits) -> bool:
    t = now.time()
    start, end = limits.session_start_utc, limits.session_end_utc
    if start <= end:
        return start <= t <= end
    # window crosses midnight
    return t >= start or t <= end


def evaluate_intent(
    intent: TradeIntent,
    state: PortfolioState,
    limits: RiskLimits,
    clock: Clock,
    mark_price: Decimal | None,
    allocate_order_id: OrderId,
) -> RiskResult:
    if limits.kill_switch:
        return RiskResult(
            approved=False,
            reason_code="KILL_SWITCH",
            reason_detail="Kill switch enabled",
        )

    now = clock.utc_now()
    if not _in_session(now, limits):
        return RiskResult(
            approved=False,
            reason_code="OUTSIDE_SESSION",
            reason_detail="Trading session closed",
        )

    if _duplicate_open_order(state, intent):
        return RiskResult(
            approved=False,
            reason_code="DUPLICATE_OPEN_ORDER",
            reason_detail="Open order exists for same instrument and side",
        )

    notional = _notional(intent, mark_price)
    if notional > limits.max_notional_per_trade:
        return RiskResult(
            approved=False,
            reason_code="MAX_NOTIONAL",
            reason_detail=str(notional),
        )

    proj = _projected_position_abs(state, intent)
    if proj > limits.max_position_abs:
        return RiskResult(
            approved=False,
            reason_code="MAX_POSITION",
            reason_detail=str(proj),
        )

    return RiskResult(approved=True, order_id=allocate_order_id)


class RiskEngine:
    """Thin wrapper for dependency injection / future Java port."""

    def __init__(self, limits: RiskLimits) -> None:
        self._limits = limits

    def evaluate(
        self,
        intent: TradeIntent,
        state: PortfolioState,
        clock: Clock,
        mark_price: Decimal | None,
        allocate_order_id: OrderId,
    ) -> RiskResult:
        return evaluate_intent(
            intent, state, self._limits, clock, mark_price, allocate_order_id
        )
