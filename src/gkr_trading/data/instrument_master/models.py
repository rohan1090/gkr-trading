from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field

from gkr_trading.core.schemas.enums import AssetClass, InstrumentStatus, OptionRight
from gkr_trading.core.schemas.ids import InstrumentId


class InstrumentRecord(BaseModel):
    model_config = {"frozen": True}

    instrument_id: InstrumentId
    asset_class: AssetClass
    canonical_symbol: str
    vendor_symbol: str | None = None
    underlying_instrument_id: InstrumentId | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    right: OptionRight | None = None
    contract_month: str | None = None
    multiplier: Decimal = Field(default=Decimal("1"))
    exchange: str | None = None
    currency: str = "USD"
    status: InstrumentStatus = InstrumentStatus.ACTIVE
