from __future__ import annotations

from decimal import Decimal

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.types import EventType
from gkr_trading.core.events.validate import (
    violations_fill_against_open_order,
    violations_for_order_submitted,
)
from gkr_trading.core.portfolio.anomalies import PortfolioAnomaly
from gkr_trading.core.portfolio.exceptions import StrictReplayError
from gkr_trading.core.portfolio.models import FillRecord, OpenOrder, PortfolioState, RejectedAction
from gkr_trading.core.schemas.enums import OrderLifecycleState, OrderSide
from gkr_trading.core.schemas.ids import FillId, InstrumentId, OrderId


def _compute_unrealized(
    positions: dict[str, Decimal],
    avg_entry: dict[str, Decimal],
    marks: dict[str, Decimal],
) -> Decimal:
    total = Decimal("0")
    for iid, q in positions.items():
        if q == 0:
            continue
        mark = marks.get(iid)
        if mark is None:
            continue
        avg = avg_entry.get(iid, Decimal("0"))
        if q > 0:
            total += (mark - avg) * q
        else:
            total += (avg - mark) * abs(q)
    return total


def _append_anomaly(
    anomalies: list[PortfolioAnomaly] | None,
    code: str,
    message: str,
    event: CanonicalEvent | None = None,
    event_index: int | None = None,
) -> None:
    if anomalies is None:
        return
    anomalies.append(
        PortfolioAnomaly(
            code=code,
            message=message,
            event_type=event.event_type.value if event else None,
            event_index=event_index,
        )
    )


def _strict_fail(strict: bool, code: str, message: str) -> None:
    if strict:
        raise StrictReplayError(code, message)


def _apply_fill_to_instrument(
    q: Decimal,
    avg: Decimal,
    side: OrderSide,
    fill_qty: Decimal,
    price: Decimal,
    cash: Decimal,
    realized: Decimal,
    fees: Decimal,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Return (new_q, new_avg, new_cash, new_realized)."""
    remaining = fill_qty
    fee_left = fees

    def take_fee() -> Decimal:
        nonlocal fee_left
        f = fee_left
        fee_left = Decimal("0")
        return f

    if side == OrderSide.BUY:
        while remaining > 0:
            if q >= 0:
                q = q + remaining
                if q == remaining and avg == 0:
                    avg = price
                else:
                    avg = (
                        ((q - remaining) * avg + remaining * price) / q
                        if q != 0
                        else Decimal("0")
                    )
                cash = cash - remaining * price - take_fee()
                remaining = Decimal("0")
            else:
                cover = min(remaining, -q)
                realized = realized + (avg - price) * cover
                cash = cash - cover * price - take_fee()
                q = q + cover
                remaining = remaining - cover
                if q == 0:
                    avg = Decimal("0")
    else:
        while remaining > 0:
            if q <= 0:
                abs_new = abs(q) + remaining
                if q == 0:
                    avg = price
                else:
                    avg = (abs(q) * avg + remaining * price) / abs_new
                q = q - remaining
                cash = cash + remaining * price - take_fee()
                remaining = Decimal("0")
            else:
                sell = min(remaining, q)
                realized = realized + (price - avg) * sell
                cash = cash + sell * price - take_fee()
                q = q - sell
                remaining = remaining - sell
                if q == 0:
                    avg = Decimal("0")

    return q, avg, cash, realized


def _apply_fill_economics(
    state: PortfolioState,
    instrument_id: InstrumentId,
    side: OrderSide,
    qty: Decimal,
    price: Decimal,
    fees: Decimal,
    order_id: OrderId,
    fill_id: FillId,
    fill_ts_utc: str,
    open_orders: dict[str, OpenOrder],
    order_lifecycle: dict[str, OrderLifecycleState],
) -> PortfolioState:
    """Apply position/cash/fill_history; caller supplies final open_orders and order_lifecycle maps."""
    iid = str(instrument_id)
    q = state.positions.get(iid, Decimal("0"))
    avg = state.avg_entry.get(iid, Decimal("0"))
    cash = state.cash
    realized = state.realized_pnl

    nq, navg, ncash, nreal = _apply_fill_to_instrument(
        q, avg, side, qty, price, cash, realized, fees
    )

    positions = dict(state.positions)
    avg_entry = dict(state.avg_entry)
    if nq == 0:
        positions.pop(iid, None)
        avg_entry.pop(iid, None)
    else:
        positions[iid] = nq
        avg_entry[iid] = navg

    fid_str = str(fill_id)
    new_fills = state.applied_fill_ids | {fid_str}
    fr = FillRecord(
        fill_id=fill_id,
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        qty=qty,
        price=price,
        fees=fees,
        fill_ts_utc=fill_ts_utc,
    )
    unreal = _compute_unrealized(positions, avg_entry, state.mark_prices)
    return state.copy_with(
        cash=ncash,
        positions=positions,
        avg_entry=avg_entry,
        realized_pnl=nreal,
        unrealized_pnl=unreal,
        open_orders=open_orders,
        order_lifecycle=order_lifecycle,
        applied_fill_ids=new_fills,
        fill_history=state.fill_history + (fr,),
    )


def _apply_fill_received(
    state: PortfolioState,
    event: CanonicalEvent,
    *,
    strict: bool,
    anomalies: list[PortfolioAnomaly] | None,
    event_index: int | None,
) -> PortfolioState:
    p = event.payload
    fill_id = FillId(str(p.fill_id))
    fid_str = str(fill_id)
    if fid_str in state.applied_fill_ids:
        return state

    oid = OrderId(str(p.order_id))
    key = str(oid)
    instrument_id = InstrumentId(str(p.instrument_id))

    if state.order_lifecycle.get(key) == OrderLifecycleState.FILLED:
        msg = f"fill after order terminal FILLED order_id={key} fill_id={fid_str}"
        _append_anomaly(anomalies, "FILL_AFTER_FILLED", msg, event, event_index)
        _strict_fail(strict, "FILL_AFTER_FILLED", msg)
        return state

    in_open = key in state.open_orders
    v_open = violations_fill_against_open_order(state, p)
    if in_open and v_open:
        msg = "; ".join(v_open)
        _append_anomaly(anomalies, "FILL_VIOLATES_OPEN_ORDER", msg, event, event_index)
        _strict_fail(strict, "FILL_VIOLATES_OPEN_ORDER", msg)
        return state

    if not in_open:
        msg = f"orphan fill (no open order) order_id={key} fill_id={fid_str}"
        _append_anomaly(anomalies, "ORPHAN_FILL", msg, event, event_index)
        _strict_fail(strict, "ORPHAN_FILL", msg)
        return _apply_fill_economics(
            state,
            instrument_id,
            p.side,
            p.fill_qty,
            p.fill_price,
            p.fees,
            oid,
            fill_id,
            p.fill_ts_utc,
            dict(state.open_orders),
            dict(state.order_lifecycle),
        )

    oo = dict(state.open_orders)
    lc = dict(state.order_lifecycle)
    prev = oo[key]
    rem = prev.remaining_qty - p.fill_qty
    if rem <= 0:
        oo.pop(key, None)
        lc[key] = OrderLifecycleState.FILLED
    else:
        oo[key] = OpenOrder(
            order_id=prev.order_id,
            instrument_id=prev.instrument_id,
            side=prev.side,
            remaining_qty=rem,
            initial_quantity=prev.initial_quantity,
            order_type=prev.order_type,
            limit_price=prev.limit_price,
            lifecycle=OrderLifecycleState.PARTIALLY_FILLED,
        )
        lc[key] = OrderLifecycleState.PARTIALLY_FILLED

    return _apply_fill_economics(
        state,
        instrument_id,
        p.side,
        p.fill_qty,
        p.fill_price,
        p.fees,
        oid,
        fill_id,
        p.fill_ts_utc,
        oo,
        lc,
    )


def apply_canonical_event(
    state: PortfolioState,
    event: CanonicalEvent,
    *,
    strict: bool = False,
    anomalies: list[PortfolioAnomaly] | None = None,
    event_index: int | None = None,
) -> PortfolioState:
    et = event.event_type
    p = event.payload

    if et == EventType.ORDER_SUBMITTED:
        oid = OrderId(str(p.order_id))
        key = str(oid)
        v = violations_for_order_submitted(state, key)
        if v:
            msg = v[0]
            _append_anomaly(
                anomalies, "DUPLICATE_SUBMIT_AFTER_TERMINAL", msg, event, event_index
            )
            _strict_fail(strict, "DUPLICATE_SUBMIT_AFTER_TERMINAL", msg)
            return state
        if key in state.open_orders:
            return state
        oo = OpenOrder(
            order_id=oid,
            instrument_id=InstrumentId(str(p.instrument_id)),
            side=p.side,
            remaining_qty=p.quantity,
            initial_quantity=p.quantity,
            order_type=p.order_type,
            limit_price=p.limit_price,
            lifecycle=OrderLifecycleState.SUBMITTED,
        )
        ood = dict(state.open_orders)
        ood[key] = oo
        ol = dict(state.order_lifecycle)
        ol[key] = OrderLifecycleState.SUBMITTED
        return state.copy_with(open_orders=ood, order_lifecycle=ol)

    if et == EventType.ORDER_ACKNOWLEDGED:
        key = str(OrderId(str(p.order_id)))
        if key not in state.open_orders:
            return state
        cur = state.open_orders[key]
        if cur.lifecycle == OrderLifecycleState.ACKNOWLEDGED:
            return state
        if cur.lifecycle == OrderLifecycleState.SUBMITTED:
            ood = dict(state.open_orders)
            ood[key] = OpenOrder(
                order_id=cur.order_id,
                instrument_id=cur.instrument_id,
                side=cur.side,
                remaining_qty=cur.remaining_qty,
                initial_quantity=cur.initial_quantity,
                order_type=cur.order_type,
                limit_price=cur.limit_price,
                lifecycle=OrderLifecycleState.ACKNOWLEDGED,
            )
            ol = dict(state.order_lifecycle)
            ol[key] = OrderLifecycleState.ACKNOWLEDGED
            return state.copy_with(open_orders=ood, order_lifecycle=ol)
        if cur.lifecycle == OrderLifecycleState.PARTIALLY_FILLED:
            return state
        msg = f"invalid ack from lifecycle {cur.lifecycle.value} order_id={key}"
        _append_anomaly(anomalies, "INVALID_ACK_TRANSITION", msg, event, event_index)
        _strict_fail(strict, "INVALID_ACK_TRANSITION", msg)
        return state

    if et == EventType.ORDER_CANCELLED:
        p = event.payload
        oid = OrderId(str(p.order_id))
        key = str(oid)
        term = state.order_lifecycle.get(key)
        if term == OrderLifecycleState.FILLED:
            msg = f"order_cancelled after terminal FILLED order_id={key}"
            _append_anomaly(anomalies, "CANCEL_AFTER_FILLED", msg, event, event_index)
            _strict_fail(strict, "CANCEL_AFTER_FILLED", msg)
            return state
        if key not in state.open_orders:
            if term in (
                OrderLifecycleState.CANCELED,
                OrderLifecycleState.REJECTED,
                OrderLifecycleState.FILLED,
            ):
                return state
            msg = f"orphan order_cancelled order_id={key}"
            _append_anomaly(anomalies, "ORPHAN_ORDER_CANCELLED", msg, event, event_index)
            _strict_fail(strict, "ORPHAN_ORDER_CANCELLED", msg)
            return state
        oo = dict(state.open_orders)
        oo.pop(key, None)
        lc = dict(state.order_lifecycle)
        lc[key] = OrderLifecycleState.CANCELED
        return state.copy_with(open_orders=oo, order_lifecycle=lc)

    if et == EventType.ORDER_REJECTED:
        p = event.payload
        oid = OrderId(str(p.order_id))
        key = str(oid)
        term = state.order_lifecycle.get(key)
        if term == OrderLifecycleState.FILLED:
            msg = f"order_rejected after terminal FILLED order_id={key}"
            _append_anomaly(anomalies, "REJECT_AFTER_FILLED", msg, event, event_index)
            _strict_fail(strict, "REJECT_AFTER_FILLED", msg)
            return state
        if key not in state.open_orders:
            if term in (
                OrderLifecycleState.CANCELED,
                OrderLifecycleState.REJECTED,
                OrderLifecycleState.FILLED,
            ):
                return state
            msg = f"orphan order_rejected order_id={key}"
            _append_anomaly(anomalies, "ORPHAN_ORDER_REJECTED", msg, event, event_index)
            _strict_fail(strict, "ORPHAN_ORDER_REJECTED", msg)
            return state
        oo = dict(state.open_orders)
        oo.pop(key, None)
        lc = dict(state.order_lifecycle)
        lc[key] = OrderLifecycleState.REJECTED
        return state.copy_with(open_orders=oo, order_lifecycle=lc)

    if et == EventType.FILL_RECEIVED:
        return _apply_fill_received(
            state, event, strict=strict, anomalies=anomalies, event_index=event_index
        )

    if et == EventType.RISK_REJECTED:
        ra = RejectedAction(
            intent_id=str(p.intent_id),
            reason_code=p.reason_code,
            reason_detail=p.reason_detail,
        )
        return state.copy_with(rejected_actions=state.rejected_actions + (ra,))

    if et == EventType.PORTFOLIO_UPDATED:
        marks = {k: Decimal(str(v)) for k, v in p.mark_prices.items()}
        pos = {k: Decimal(str(v)) for k, v in p.positions.items()}
        avgc = {k: Decimal(str(v)) for k, v in p.avg_cost.items()}
        unreal = _compute_unrealized(pos, avgc, marks)
        return state.copy_with(
            cash=Decimal(str(p.cash)),
            positions=pos,
            avg_entry=avgc,
            realized_pnl=Decimal(str(p.realized_pnl)),
            unrealized_pnl=unreal,
            mark_prices=marks,
        )

    if et == EventType.MARKET_DATA_RECEIVED:
        iid = str(p.instrument_id)
        marks = dict(state.mark_prices)
        marks[iid] = Decimal(str(p.close))
        unreal = _compute_unrealized(state.positions, state.avg_entry, marks)
        return state.copy_with(mark_prices=marks, unrealized_pnl=unreal)

    return state
