"""Per-order state machine tracking OrderStatus transitions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from gkr_trading.core.order_model import OrderStatus, validate_transition


@dataclass
class OrderState:
    """Tracked state for a single order."""
    client_order_id: str
    status: OrderStatus
    venue_order_id: Optional[str] = None
    filled_qty: int = 0
    remaining_qty: int = 0
    history: List[OrderStatus] = field(default_factory=list)

    def transition(self, new_status: OrderStatus) -> bool:
        """Attempt a status transition. Returns True if legal and applied."""
        if not validate_transition(self.status, new_status):
            return False
        self.history.append(self.status)
        self.status = new_status
        return True


class OrderLifecycleTracker:
    """Tracks all orders in a session with legal state transitions."""

    def __init__(self) -> None:
        self._orders: Dict[str, OrderState] = {}

    def register(self, client_order_id: str, quantity: int) -> OrderState:
        """Register a new order starting at PENDING_LOCAL."""
        state = OrderState(
            client_order_id=client_order_id,
            status=OrderStatus.PENDING_LOCAL,
            remaining_qty=quantity,
        )
        self._orders[client_order_id] = state
        return state

    def transition(self, client_order_id: str, new_status: OrderStatus) -> bool:
        """Transition an order to a new status. Returns False if illegal or unknown order."""
        state = self._orders.get(client_order_id)
        if state is None:
            return False
        return state.transition(new_status)

    def record_fill(self, client_order_id: str, fill_qty: int) -> bool:
        """Record a partial or full fill."""
        state = self._orders.get(client_order_id)
        if state is None:
            return False
        state.filled_qty += fill_qty
        state.remaining_qty = max(0, state.remaining_qty - fill_qty)
        if state.remaining_qty == 0:
            state.transition(OrderStatus.FILLED)
        else:
            state.transition(OrderStatus.PARTIALLY_FILLED)
        return True

    def get(self, client_order_id: str) -> Optional[OrderState]:
        return self._orders.get(client_order_id)

    def get_active_orders(self) -> List[OrderState]:
        return [s for s in self._orders.values() if not s.status.is_terminal]

    def get_all_orders(self) -> Dict[str, OrderState]:
        return dict(self._orders)
