from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, Field, model_validator

from gkr_trading.core.schemas.enums import OrderSide, OrderType
from gkr_trading.core.schemas.ids import (
    FillId,
    InstrumentId,
    IntentId,
    OrderId,
    SessionId,
    deterministic_fill_id_v1,
    fill_id_from_broker_execution,
)


class MarketDataReceivedPayload(BaseModel):
    model_config = {"frozen": True}

    instrument_id: InstrumentId
    timeframe: str
    bar_ts_utc: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Field(default=Decimal("0"))


class SignalGeneratedPayload(BaseModel):
    model_config = {"frozen": True}

    strategy_name: str
    instrument_id: InstrumentId
    signal_name: str
    strength: Decimal | None = None


class TradeIntentCreatedPayload(BaseModel):
    model_config = {"frozen": True}

    intent_id: IntentId
    instrument_id: InstrumentId
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    limit_price: Decimal | None
    strategy_name: str


class RiskApprovedPayload(BaseModel):
    model_config = {"frozen": True}

    intent_id: IntentId
    order_id: OrderId


class RiskRejectedPayload(BaseModel):
    model_config = {"frozen": True}

    intent_id: IntentId
    reason_code: str
    reason_detail: str | None = None


class OrderSubmittedPayload(BaseModel):
    model_config = {"frozen": True}

    order_id: OrderId
    instrument_id: InstrumentId
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    limit_price: Decimal | None


class OrderAcknowledgedPayload(BaseModel):
    model_config = {"frozen": True}

    order_id: OrderId
    broker_order_id: str | None = None


class FillReceivedPayload(BaseModel):
    model_config = {"frozen": True}

    order_id: OrderId
    instrument_id: InstrumentId
    side: OrderSide
    fill_qty: Decimal
    fill_price: Decimal
    fees: Decimal = Decimal("0")
    fill_ts_utc: str
    fill_id: FillId
    broker_execution_id: str | None = None
    """Synthetic discriminator for fallback hashing (required for new writes; legacy JSON may omit)."""
    synthetic_leg_key: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_fill_id(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        d = dict(data)
        fid = d.get("fill_id")
        bex = d.get("broker_execution_id")
        if fid and bex:
            exp = str(fill_id_from_broker_execution(str(bex)))
            if str(fid) != exp:
                raise ValueError(
                    "fill_id is inconsistent with broker_execution_id under canonical policy "
                    f"(expected {exp!r}, got {fid!r})"
                )
            return d
        if fid:
            return d
        if bex:
            d["fill_id"] = str(fill_id_from_broker_execution(str(bex)))
            return d
        oid = d.get("order_id")
        fts = d.get("fill_ts_utc")
        fq = d.get("fill_qty")
        fp = d.get("fill_price")
        fee_raw = d.get("fees", "0")
        if oid is None or fts is None or fq is None or fp is None:
            raise ValueError(
                "fill_id, broker_execution_id, or legacy fill fields (order_id, fill_ts_utc, "
                "fill_qty, fill_price) are required"
            )
        try:
            fq_d = Decimal(str(fq))
            fp_d = Decimal(str(fp))
            fee_d = Decimal(str(fee_raw))
        except (InvalidOperation, ValueError) as e:
            raise ValueError(
                f"invalid decimal in legacy fill payload: {e}"
            ) from e
        if d.get("synthetic_leg_key") is None:
            d["fill_id"] = str(
                deterministic_fill_id_v1(str(oid), str(fts), fq_d, fp_d, fee_d, salt="")
            )
        else:
            leg = str(d["synthetic_leg_key"])
            d["fill_id"] = str(
                deterministic_fill_id_v1(
                    str(oid),
                    str(fts),
                    fq_d,
                    fp_d,
                    fee_d,
                    salt=f"|leg:{leg}",
                )
            )
        return d

    @model_validator(mode="after")
    def _broker_consistency(self) -> FillReceivedPayload:
        if self.broker_execution_id:
            exp = fill_id_from_broker_execution(self.broker_execution_id)
            if str(self.fill_id) != str(exp):
                raise ValueError(
                    "fill_id must equal exec:{broker_execution_id} when broker_execution_id is set"
                )
        return self


class OrderCancelledPayload(BaseModel):
    model_config = {"frozen": True}

    order_id: OrderId
    reason_code: str | None = None
    """Quantity cancelled at broker (optional; remainder after partial fills)."""
    cancelled_qty: Decimal | None = None


class BrokerOrderRejectedPayload(BaseModel):
    """Broker/exchange rejected a working order (distinct from RISK_REJECTED on intent)."""

    model_config = {"frozen": True}

    order_id: OrderId
    reason_code: str
    reason_detail: str | None = None


class PortfolioUpdatedPayload(BaseModel):
    """Snapshot fields for deterministic replay of marks and PnL."""

    model_config = {"frozen": True}

    cash: Decimal
    positions: dict[str, Decimal]  # instrument_id -> qty
    avg_cost: dict[str, Decimal]
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    mark_prices: dict[str, Decimal]  # instrument_id -> mark for unrealized


class SessionStartedPayload(BaseModel):
    model_config = {"frozen": True}

    session_id: SessionId
    mode: str


class SessionStoppedPayload(BaseModel):
    model_config = {"frozen": True}

    session_id: SessionId
    reason: str | None = None


class ReplayCompletedPayload(BaseModel):
    model_config = {"frozen": True}

    session_id: SessionId
    events_replayed: int


# --- Options Lifecycle Payloads ---

class AssignmentReceivedPayload(BaseModel):
    """NOT a fill. Assignment from OCC/broker delivered asynchronously."""
    model_config = {"frozen": True}

    event_id: str
    instrument_occ_symbol: str
    instrument_underlying: str
    venue: str
    contracts_assigned: int
    strike_cents: int
    right: str  # "call" or "put"
    resulting_equity_delta: int
    equity_underlying: str
    assignment_price_cents: int
    effective_date: str
    source: str  # "auto" or "manual"
    requires_operator_review: bool = False


class ExerciseProcessedPayload(BaseModel):
    """NOT a fill. Exercise of long position."""
    model_config = {"frozen": True}

    event_id: str
    instrument_occ_symbol: str
    instrument_underlying: str
    venue: str
    contracts_exercised: int
    strike_cents: int
    right: str
    resulting_equity_delta: int
    equity_underlying: str
    effective_date: str
    initiated_by: str  # "system" or "operator"


class ExpirationProcessedPayload(BaseModel):
    """NOT a fill. Option expired worthless."""
    model_config = {"frozen": True}

    event_id: str
    instrument_occ_symbol: str
    instrument_underlying: str
    venue: str
    contracts_expired: int
    moneyness_at_expiry: str  # "otm" or "atm"
    premium_paid_cents: int = 0
    premium_received_cents: int = 0
    expiry_type: str = "standard_monthly"


class OperatorCommandPayload(BaseModel):
    """Operator-issued command persisted before execution."""
    model_config = {"frozen": True}

    command_id: str
    command_type: str
    parameters: str | None = None
    operator_id: str = "cli"


class ReconciliationCompletedPayload(BaseModel):
    """Reconciliation snapshot result."""
    model_config = {"frozen": True}

    snapshot_id: str
    trigger: str
    status: str  # "clean", "break_detected", "acknowledged"
    break_count: int = 0
    blocking_break_count: int = 0


class PendingOrderRegisteredPayload(BaseModel):
    """Pending order registered in registry before API call."""
    model_config = {"frozen": True}

    client_order_id: str
    intent_id: str
    instrument_key: str  # canonical_key from InstrumentRef
    action: str
    venue: str
    quantity: int
    limit_price_cents: int | None = None


class OrderSubmissionAttemptedPayload(BaseModel):
    """API call result or timeout recorded after submission attempt."""
    model_config = {"frozen": True}

    client_order_id: str
    venue_order_id: str | None = None
    success: bool
    rejected: bool = False
    reject_reason: str | None = None
    timeout: bool = False
