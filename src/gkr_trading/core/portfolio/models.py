from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from gkr_trading.core.schemas.enums import OrderLifecycleState, OrderSide, OrderType
from gkr_trading.core.schemas.ids import FillId, InstrumentId, OrderId


@dataclass(frozen=True)
class OpenOrder:
    order_id: OrderId
    instrument_id: InstrumentId
    side: OrderSide
    remaining_qty: Decimal
    initial_quantity: Decimal
    order_type: OrderType
    limit_price: Decimal | None = None
    lifecycle: OrderLifecycleState = OrderLifecycleState.SUBMITTED


@dataclass(frozen=True)
class FillRecord:
    fill_id: FillId
    order_id: OrderId
    instrument_id: InstrumentId
    side: OrderSide
    qty: Decimal
    price: Decimal
    fees: Decimal
    fill_ts_utc: str


@dataclass(frozen=True)
class RejectedAction:
    intent_id: str
    reason_code: str
    reason_detail: str | None


@dataclass
class PortfolioState:
    cash: Decimal
    positions: dict[str, Decimal]  # instrument_id -> signed qty
    avg_entry: dict[str, Decimal]  # instrument_id -> positive avg entry price for current net
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    mark_prices: dict[str, Decimal]
    open_orders: dict[str, OpenOrder]  # order_id
    applied_fill_ids: frozenset[str]
    order_lifecycle: dict[str, OrderLifecycleState]  # order_id str -> latest known lifecycle
    fill_history: tuple[FillRecord, ...]
    rejected_actions: tuple[RejectedAction, ...]

    @staticmethod
    def initial(starting_cash: Decimal) -> PortfolioState:
        return PortfolioState(
            cash=starting_cash,
            positions={},
            avg_entry={},
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            mark_prices={},
            open_orders={},
            applied_fill_ids=frozenset(),
            order_lifecycle={},
            fill_history=(),
            rejected_actions=(),
        )

    def copy_with(self, **kwargs: object) -> PortfolioState:
        return PortfolioState(
            cash=kwargs.get("cash", self.cash),  # type: ignore[arg-type]
            positions=dict(kwargs.get("positions", self.positions)),  # type: ignore[arg-type]
            avg_entry=dict(kwargs.get("avg_entry", self.avg_entry)),  # type: ignore[arg-type]
            realized_pnl=kwargs.get("realized_pnl", self.realized_pnl),  # type: ignore[arg-type]
            unrealized_pnl=kwargs.get("unrealized_pnl", self.unrealized_pnl),  # type: ignore[arg-type]
            mark_prices=dict(kwargs.get("mark_prices", self.mark_prices)),  # type: ignore[arg-type]
            open_orders=dict(kwargs.get("open_orders", self.open_orders)),  # type: ignore[arg-type]
            applied_fill_ids=kwargs.get("applied_fill_ids", self.applied_fill_ids),  # type: ignore[arg-type]
            order_lifecycle=dict(kwargs.get("order_lifecycle", self.order_lifecycle)),  # type: ignore[arg-type]
            fill_history=kwargs.get("fill_history", self.fill_history),  # type: ignore[arg-type]
            rejected_actions=kwargs.get("rejected_actions", self.rejected_actions),  # type: ignore[arg-type]
        )
