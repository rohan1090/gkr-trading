from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import InstrumentId, OrderId


@dataclass(frozen=True)
class SubmitRequest:
    order_id: OrderId
    instrument_id: InstrumentId
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    limit_price: Decimal | None = None
    """Executable symbol for REST broker (e.g. Alpaca). Canonical identity remains instrument_id."""
    executable_broker_symbol: str | None = None
    """Wall/venue context for submit() timestamps (e.g. current bar_ts_utc)."""
    context_ts_utc: str | None = None


@dataclass(frozen=True)
class SubmissionResult:
    """Normalized outcome of a synchronous submit HTTP/RPC call (no portfolio mutation)."""

    occurred_at_utc: str
    broker_order_id: str | None = None
    rejected: bool = False
    reject_reason_code: str | None = None
    reject_reason_detail: str | None = None


@dataclass(frozen=True)
class BrokerReconciliationCursor:
    """Adapter-owned bookmark (e.g. Alpaca activity id). Opaque to core/replay."""

    token: str = ""


class BrokerSyncPhase(StrEnum):
    STARTUP = "startup"
    PRE_BAR = "pre_bar"
    POST_SUBMIT = "post_submit"
    POST_BAR = "post_bar"


@dataclass(frozen=True)
class BrokerPollHints:
    """Hints for synthetic/mock pricing; real adapters may ignore some fields."""

    bar_ts_utc: str | None = None
    reference_price: Decimal | None = None
    default_occurred_at_utc: str | None = None


@dataclass(frozen=True)
class BrokerOrderRejectedFact:
    client_order_id: OrderId
    reason_code: str
    occurred_at_utc: str
    reason_detail: str | None = None

    @property
    def kind(self) -> str:
        return "reject"

    def tie_breaker(self) -> str:
        return str(self.client_order_id)


@dataclass(frozen=True)
class BrokerOrderCancelledFact:
    client_order_id: OrderId
    occurred_at_utc: str
    reason_code: str | None = None
    cancelled_qty: Decimal | None = None

    @property
    def kind(self) -> str:
        return "cancel"

    def tie_breaker(self) -> str:
        return str(self.client_order_id)


@dataclass(frozen=True)
class BrokerFillFact:
    client_order_id: OrderId
    instrument_id: InstrumentId
    side: OrderSide
    quantity: Decimal
    price: Decimal
    fees: Decimal
    fill_ts_utc: str
    occurred_at_utc: str
    broker_execution_id: str

    @property
    def kind(self) -> str:
        return "fill"

    def tie_breaker(self) -> str:
        return self.broker_execution_id


BrokerFact = BrokerOrderRejectedFact | BrokerOrderCancelledFact | BrokerFillFact


@dataclass(frozen=True)
class BrokerPollResult:
    facts: tuple[BrokerFact, ...]
    cursor: BrokerReconciliationCursor


class BrokerAdapter(Protocol):
    def submit(self, req: SubmitRequest) -> SubmissionResult: ...

    def poll_broker_facts(
        self,
        *,
        cursor: BrokerReconciliationCursor | None,
        hints: BrokerPollHints,
        phase: BrokerSyncPhase,
    ) -> BrokerPollResult: ...


class MockBrokerAdapter:
    """Deterministic paper broker: submit registers orders; fills emitted at poll fences (see flags)."""

    def __init__(
        self,
        *,
        defer_fill_to_next_pre_bar: bool = False,
        emit_duplicate_fill_on_post_bar: bool = False,
        reject_next_submit: bool = False,
        synthetic_fill_enabled: bool = True,
    ) -> None:
        self.submitted: list[SubmitRequest] = []
        self.defer_fill_to_next_pre_bar = defer_fill_to_next_pre_bar
        self.emit_duplicate_fill_on_post_bar = emit_duplicate_fill_on_post_bar
        self.reject_next_submit = reject_next_submit
        self.synthetic_fill_enabled = synthetic_fill_enabled
        self._awaiting_fill: dict[str, SubmitRequest] = {}
        self._last_exec_id_by_order: dict[str, str] = {}
        self._startup_queue: list[BrokerFact] = []
        self._pre_bar_queue: list[BrokerFact] = []
        self._post_bar_queue: list[BrokerFact] = []
        self._post_submit_queue: list[BrokerFact] = []
        self._cursor_seq = 0

    def inject_startup_fact(self, fact: BrokerFact) -> None:
        self._startup_queue.append(fact)

    def inject_pre_bar_fact(self, fact: BrokerFact) -> None:
        self._pre_bar_queue.append(fact)

    def inject_post_bar_fact(self, fact: BrokerFact) -> None:
        self._post_bar_queue.append(fact)

    def inject_post_submit_fact(self, fact: BrokerFact) -> None:
        self._post_submit_queue.append(fact)

    def submit(self, req: SubmitRequest) -> SubmissionResult:
        self.submitted.append(req)
        ts = req.context_ts_utc or _hints_ts(
            BrokerPollHints(bar_ts_utc=None, default_occurred_at_utc=None)
        )
        if self.reject_next_submit:
            self.reject_next_submit = False
            return SubmissionResult(
                occurred_at_utc=ts,
                broker_order_id=None,
                rejected=True,
                reject_reason_code="MOCK_REJECT",
                reject_reason_detail="MockBrokerAdapter.reject_next_submit",
            )
        if self.synthetic_fill_enabled:
            self._awaiting_fill[str(req.order_id)] = req
        return SubmissionResult(
            occurred_at_utc=ts,
            broker_order_id=f"MOCK-{req.order_id}",
            rejected=False,
        )

    def poll_broker_facts(
        self,
        *,
        cursor: BrokerReconciliationCursor | None,
        hints: BrokerPollHints,
        phase: BrokerSyncPhase,
    ) -> BrokerPollResult:
        facts: list[BrokerFact] = []
        if phase == BrokerSyncPhase.STARTUP:
            facts.extend(self._drain(self._startup_queue))
        if phase == BrokerSyncPhase.PRE_BAR:
            facts.extend(self._drain(self._pre_bar_queue))
            if self.defer_fill_to_next_pre_bar:
                facts.extend(self._emit_fills_for_awaiting(hints))
        if phase == BrokerSyncPhase.POST_SUBMIT:
            facts.extend(self._drain(self._post_submit_queue))
            if not self.defer_fill_to_next_pre_bar:
                facts.extend(self._emit_fills_for_awaiting(hints))
        if phase == BrokerSyncPhase.POST_BAR:
            facts.extend(self._drain(self._post_bar_queue))
            if self.emit_duplicate_fill_on_post_bar:
                facts.extend(self._duplicate_last_fills(hints))

        self._cursor_seq += 1
        tok = (cursor.token + f":{self._cursor_seq}") if cursor else str(self._cursor_seq)
        return BrokerPollResult(tuple(facts), BrokerReconciliationCursor(token=tok))

    def _drain(self, q: list[BrokerFact]) -> list[BrokerFact]:
        out = list(q)
        q.clear()
        return out

    def _emit_fills_for_awaiting(self, hints: BrokerPollHints) -> list[BrokerFillFact]:
        if not self.synthetic_fill_enabled:
            return []
        ts = _hints_ts(hints)
        price = hints.reference_price if hints.reference_price is not None else Decimal("100")
        bar_ts = hints.bar_ts_utc or ts
        out: list[BrokerFillFact] = []
        for oid, req in list(self._awaiting_fill.items()):
            bexec = f"mock-exec-{oid}-{bar_ts}"
            self._last_exec_id_by_order[oid] = bexec
            out.append(
                BrokerFillFact(
                    client_order_id=req.order_id,
                    instrument_id=req.instrument_id,
                    side=req.side,
                    quantity=req.quantity,
                    price=price,
                    fees=Decimal("0"),
                    fill_ts_utc=bar_ts,
                    occurred_at_utc=ts,
                    broker_execution_id=bexec,
                )
            )
            del self._awaiting_fill[oid]
        return out

    def _duplicate_last_fills(self, hints: BrokerPollHints) -> list[BrokerFillFact]:
        """Test hook: re-emit the same broker_execution_id as the last fill per order."""
        ts = _hints_ts(hints)
        bar_ts = hints.bar_ts_utc or ts
        price = hints.reference_price if hints.reference_price is not None else Decimal("100")
        out: list[BrokerFillFact] = []
        for oid, bexec in self._last_exec_id_by_order.items():
            req = next((r for r in self.submitted if str(r.order_id) == oid), None)
            if req is None:
                continue
            out.append(
                BrokerFillFact(
                    client_order_id=OrderId(oid),
                    instrument_id=req.instrument_id,
                    side=req.side,
                    quantity=req.quantity,
                    price=price,
                    fees=Decimal("0"),
                    fill_ts_utc=bar_ts,
                    occurred_at_utc=ts,
                    broker_execution_id=bexec,
                )
            )
        return out


def _hints_ts(hints: BrokerPollHints) -> str:
    return hints.default_occurred_at_utc or hints.bar_ts_utc or "2024-01-01T00:00:00Z"
