from __future__ import annotations

from enum import StrEnum


class AssetClass(StrEnum):
    EQUITY = "equity"
    FUTURE = "future"
    OPTION = "option"
    FX = "fx"
    CRYPTO = "crypto"


class OptionRight(StrEnum):
    CALL = "C"
    PUT = "P"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(StrEnum):
    PENDING_NEW = "pending_new"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class OrderLifecycleState(StrEnum):
    """Canonical reducer-side lifecycle (V1). Broker-specific statuses map here."""

    NEW = "new"  # reserved — V1 orders begin at SUBMITTED when `order_submitted` is applied
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"  # no V1 event yet; scaffold for adapters
    REJECTED = "rejected"  # broker reject; scaffold — no V1 event yet


class InstrumentStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    EXPIRED = "expired"


class Timeframe(StrEnum):
    M1 = "1m"
    M5 = "5m"
    H1 = "1h"
    D1 = "1d"
