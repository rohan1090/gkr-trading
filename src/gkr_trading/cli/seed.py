from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import sqlite3

from gkr_trading.core.schemas.enums import AssetClass, InstrumentStatus, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId
from gkr_trading.data.ingestion.bars import ingest_equity_bars
from gkr_trading.data.instrument_master.models import InstrumentRecord
from gkr_trading.data.instrument_master.repository import InstrumentRepository
from gkr_trading.data.market_store.ddl import init_schema
from gkr_trading.data.market_store.repository import MarketDataRepository
from gkr_trading.data.universe_registry.repository import UniverseRepository
from gkr_trading.persistence.db import open_sqlite

DEMO_EQUITY_ID = InstrumentId("00000000-0000-4000-8000-000000000001")
DEMO_FUTURE_ID = InstrumentId("00000000-0000-4000-8000-000000000002")


def initialize_database(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = open_sqlite(db_path)
    init_schema(conn)
    return conn


def seed_instruments(conn: sqlite3.Connection) -> None:
    repo = InstrumentRepository(conn)
    # NOTE: The demo equity must map to a real tradable Alpaca symbol.
    # Alpaca rejects placeholder symbols like DEMO ("asset DEMO is not active").
    repo.upsert(
        InstrumentRecord(
            instrument_id=DEMO_EQUITY_ID,
            asset_class=AssetClass.EQUITY,
            canonical_symbol="SPY",
            vendor_symbol="SPY.US",
            multiplier=Decimal("1"),
            exchange="XNYS",
            currency="USD",
            status=InstrumentStatus.ACTIVE,
        )
    )
    repo.upsert(
        InstrumentRecord(
            instrument_id=DEMO_FUTURE_ID,
            asset_class=AssetClass.FUTURE,
            canonical_symbol="DEMOFUT",
            vendor_symbol="DEMOFUTZ4",
            contract_month="202412",
            multiplier=Decimal("50"),
            exchange="XCME",
            currency="USD",
            status=InstrumentStatus.ACTIVE,
            expiry=date(2024, 12, 20),
        )
    )
    ur = UniverseRepository(conn)
    ur.create_universe("demo")
    ur.add_member("demo", DEMO_EQUITY_ID)
    ur.add_member("demo", DEMO_FUTURE_ID)


def seed_equity_bars(conn: sqlite3.Connection) -> None:
    """Bars designed so a two-bar down-then-up triggers BUY on third bar."""
    repo = MarketDataRepository(conn)
    tf = Timeframe.D1.value
    bars = [
        ("2024-01-02T21:00:00Z", Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1000")),
        ("2024-01-03T21:00:00Z", Decimal("100"), Decimal("100"), Decimal("97"), Decimal("98"), Decimal("1100")),
        ("2024-01-04T21:00:00Z", Decimal("98"), Decimal("102"), Decimal("98"), Decimal("101"), Decimal("1200")),
        ("2024-01-05T21:00:00Z", Decimal("101"), Decimal("105"), Decimal("100"), Decimal("104"), Decimal("900")),
    ]
    ingest_equity_bars(repo, DEMO_EQUITY_ID, tf, bars)
