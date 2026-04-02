from __future__ import annotations

from gkr_trading.core.events.payloads import FillReceivedPayload
from gkr_trading.core.portfolio.models import PortfolioState
from gkr_trading.core.schemas.enums import OrderLifecycleState


TERMINAL_LIFECYCLE: frozenset[OrderLifecycleState] = frozenset(
    {
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCELED,
        OrderLifecycleState.REJECTED,
    }
)


def violations_for_order_submitted(
    state: PortfolioState, order_id_str: str
) -> list[str]:
    """Violations if a new working order must not be opened for this order_id."""
    st = state.order_lifecycle.get(order_id_str)
    if st in TERMINAL_LIFECYCLE:
        return [f"DUPLICATE_SUBMIT_AFTER_TERMINAL: order_id={order_id_str} state={st.value}"]
    return []


def violations_fill_against_open_order(
    state: PortfolioState,
    p: FillReceivedPayload,
) -> list[str]:
    """Violations when an open order exists and the fill must match it."""
    key = str(p.order_id)
    if key not in state.open_orders:
        return []
    oo = state.open_orders[key]
    out: list[str] = []
    if p.side != oo.side:
        out.append(f"FILL_SIDE_MISMATCH: order_id={key}")
    if p.instrument_id != oo.instrument_id:
        out.append(f"FILL_INSTRUMENT_MISMATCH: order_id={key}")
    if p.fill_qty > oo.remaining_qty:
        out.append(
            f"FILL_EXCEEDS_REMAINING: order_id={key} fill_qty={p.fill_qty} remaining={oo.remaining_qty}"
        )
    return out
