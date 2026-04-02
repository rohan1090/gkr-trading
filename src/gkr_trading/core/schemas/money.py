from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from pydantic import PlainSerializer

Money = Annotated[
    Decimal,
    PlainSerializer(lambda v: str(v), return_type=str),
]


def to_decimal(x: str | float | int | Decimal) -> Decimal:
    return Decimal(str(x))
