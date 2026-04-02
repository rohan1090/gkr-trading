from __future__ import annotations

from typing import Callable

from gkr_trading.core.schemas.enums import AssetClass
from gkr_trading.core.schemas.ids import InstrumentId
from gkr_trading.data.instrument_master.models import InstrumentRecord
from gkr_trading.data.instrument_master.repository import InstrumentRepository


class InstrumentSymbolResolutionError(LookupError):
    """No executable broker symbol could be derived for an instrument_id."""


def resolve_alpaca_equity_executable_symbol(rec: InstrumentRecord) -> str:
    """
    Map instrument master row to an Alpaca US equity order symbol.
    Canonical identity stays instrument_id; this string is only for REST submission.
    Policy: prefer vendor_symbol, else canonical_symbol; strip trailing '.US' for equities.
    """
    raw = (rec.vendor_symbol or rec.canonical_symbol or "").strip()
    if not raw:
        raise InstrumentSymbolResolutionError(
            f"empty symbol fields for instrument_id={rec.instrument_id!r}"
        )
    if rec.asset_class == AssetClass.EQUITY and raw.endswith(".US"):
        return raw[: -len(".US")]
    return raw


def make_alpaca_equity_symbol_resolver(
    repo: InstrumentRepository,
) -> Callable[[InstrumentId], str]:
    """Closure used by paper runtime when symbol resolution is required."""

    def resolve(instrument_id: InstrumentId) -> str:
        rec = repo.get(instrument_id)
        if rec is None:
            raise InstrumentSymbolResolutionError(
                f"instrument_id not in master: {instrument_id!r}"
            )
        if rec.asset_class != AssetClass.EQUITY:
            raise InstrumentSymbolResolutionError(
                f"Phase 1 Alpaca path supports equities only; got {rec.asset_class!r} "
                f"for instrument_id={instrument_id!r}"
            )
        return resolve_alpaca_equity_executable_symbol(rec)

    return resolve
