"""Alpaca hardening: pagination, persistence, uncertainty resolve, rehydrate (no network)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from gkr_trading.core.events.builders import (
    order_acknowledged,
    order_submitted,
    session_started,
)
from gkr_trading.core.schemas.enums import OrderSide, OrderType, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId, OrderId, SessionId
from gkr_trading.data.market_store.ddl import init_schema
from gkr_trading.live.alpaca_config import AlpacaPaperConfig
from gkr_trading.live.alpaca_http import AlpacaSubmitUncertaintyError, AlpacaSubmitUnresolvedError
from gkr_trading.live.alpaca_pagination import iter_fill_activity_pages_desc, iter_order_pages_desc
from gkr_trading.live.alpaca_paper_adapter import AlpacaPaperAdapter
from gkr_trading.live.alpaca_rehydrate import rehydrate_tracked_orders_from_events
from gkr_trading.live.broker_adapter import BrokerPollHints, BrokerSyncPhase, SubmitRequest
from gkr_trading.persistence.broker_reconciliation_store import (
    load_broker_reconciliation_payload,
    save_broker_reconciliation_payload,
)

CFG = AlpacaPaperConfig(api_key="k", secret_key="s")
IID = InstrumentId("00000000-0000-4000-8000-00000000cc01")
OID = OrderId("00000000-0000-4000-8000-00000000cc02")


class SequentialAlpacaHttp:
    """Each GET /v2/orders or activities pops the next scripted page (matches iter_* pagination)."""

    def __init__(self) -> None:
        self.order_pages: list[list[dict[str, Any]]] = []
        self.activity_pages: list[list[dict[str, Any]]] = []
        self.submit_responses: list[Any] = []
        self.calls: list[tuple[str, str]] = []

    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((method, path))
        if method == "POST" and path == "/v2/orders":
            if not self.submit_responses:
                raise RuntimeError("no submit response queued")
            item = self.submit_responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        if method == "GET" and path == "/v2/orders":
            if not self.order_pages:
                return []
            return self.order_pages.pop(0)
        if method == "GET" and "/account/activities" in path:
            if not self.activity_pages:
                return []
            return self.activity_pages.pop(0)
        return []


def test_iter_order_pages_two_pages() -> None:
    http = SequentialAlpacaHttp()
    http.order_pages = [
        [{"id": f"p0-{i}", "client_order_id": "x"} for i in range(100)],
        [{"id": "old-1", "client_order_id": "y"}],
    ]
    pages, n = iter_order_pages_desc(http, max_pages=10)
    assert n == 2
    assert len(pages[0]) == 100
    assert len(pages[1]) == 1


def test_iter_fill_activities_multi_page() -> None:
    http = SequentialAlpacaHttp()
    http.activity_pages = [
        [
            {
                "id": f"f{i}",
                "activity_type": "FILL",
                "order_id": "alp1",
                "qty": "1",
                "price": "10",
                "side": "buy",
                "transaction_time": "2024-01-01T00:00:00Z",
            }
            for i in range(100)
        ],
        [
            {
                "id": "f-old",
                "activity_type": "FILL",
                "order_id": "alp1",
                "qty": "1",
                "price": "9",
                "side": "buy",
                "transaction_time": "2023-12-01T00:00:00Z",
            }
        ],
    ]
    pages, n = iter_fill_activity_pages_desc(http, max_pages=5)
    assert n == 2
    assert len(pages[0]) == 100
    assert len(pages[1]) == 1


def test_persist_roundtrip_adapter_state(tmp_path) -> None:
    import sqlite3

    db = str(tmp_path / "b.db")
    conn = sqlite3.connect(db)
    init_schema(conn)
    ad = AlpacaPaperAdapter(CFG, http=SequentialAlpacaHttp())
    req = SubmitRequest(
        order_id=OID,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        order_type=OrderType.MARKET,
        limit_price=None,
        executable_broker_symbol="D",
        context_ts_utc="2024-01-01T00:00:00Z",
    )
    ad._by_client[str(OID)] = req
    ad._alpaca_id_by_client[str(OID)] = "alp-x"
    ad._emitted_fill_activity_ids.add("ex1")
    save_broker_reconciliation_payload(conn, "sid-1", ad.export_persisted_payload())
    loaded = load_broker_reconciliation_payload(conn, "sid-1")
    ad2 = AlpacaPaperAdapter(CFG, http=SequentialAlpacaHttp())
    ad2.import_persisted_payload(loaded or {})
    assert str(OID) in ad2._by_client
    assert ad2._alpaca_id_by_client[str(OID)] == "alp-x"
    assert "ex1" in ad2._emitted_fill_activity_ids
    conn.close()


def test_uncertainty_resolve_finds_order_on_second_page() -> None:
    http = SequentialAlpacaHttp()
    http.order_pages = [
        [{"id": str(i), "client_order_id": f"other-{i}", "status": "filled"} for i in range(100)],
        [{"id": "mine", "client_order_id": str(OID), "status": "accepted"}],
    ]
    ad = AlpacaPaperAdapter(CFG, http=http)
    req = SubmitRequest(
        order_id=OID,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.MARKET,
        limit_price=None,
        executable_broker_symbol="IBM",
        context_ts_utc="2024-01-01T00:00:00Z",
    )
    r = ad.resolve_submit_uncertainty(req)
    assert r.found
    assert r.submission_result and not r.submission_result.rejected
    assert r.submission_result.broker_order_id == "mine"
    assert ad.last_uncertainty_resolve_pages() == 2


def test_uncertainty_resolve_not_found() -> None:
    http = SequentialAlpacaHttp()
    http.order_pages = [[{"id": "x", "client_order_id": "other", "status": "filled"}]]
    ad = AlpacaPaperAdapter(CFG, http=http)
    req = SubmitRequest(
        order_id=OID,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.MARKET,
        limit_price=None,
        executable_broker_symbol="IBM",
        context_ts_utc="2024-01-01T00:00:00Z",
    )
    r = ad.resolve_submit_uncertainty(req)
    assert not r.found


def test_poll_aggregates_two_activity_pages_for_tracked_order() -> None:
    http = SequentialAlpacaHttp()
    http.order_pages = [[]]
    noise = [
        {
            "id": f"noise-{i}",
            "activity_type": "FILL",
            "order_id": "other",
            "qty": "1",
            "price": "1",
            "side": "buy",
            "transaction_time": "2024-01-01T00:00:00Z",
        }
        for i in range(100)
    ]
    http.activity_pages = [
        noise,
        [
            {
                "id": "act-deep",
                "activity_type": "FILL",
                "order_id": "alp-9",
                "qty": "1",
                "price": "50",
                "side": "buy",
                "transaction_time": "2024-01-02T00:00:00Z",
            }
        ],
    ]
    ad = AlpacaPaperAdapter(CFG, http=http)
    ad._by_client[str(OID)] = SubmitRequest(
        order_id=OID,
        instrument_id=IID,
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.MARKET,
        limit_price=None,
        executable_broker_symbol="X",
        context_ts_utc="2024-01-01T00:00:00Z",
    )
    ad._alpaca_id_by_client[str(OID)] = "alp-9"
    h = BrokerPollHints(default_occurred_at_utc="2024-01-01T00:00:00Z")
    batch = ad.poll_broker_facts(cursor=None, hints=h, phase=BrokerSyncPhase.POST_BAR)
    exec_ids = {f.broker_execution_id for f in batch.facts if f.kind == "fill"}
    assert "act-deep" in exec_ids
    assert ad._last_activity_pages_polled == 2


def test_rehydrate_open_order_with_ack() -> None:
    sid = SessionId("00000000-0000-4000-8000-00000000dd01")
    ts = "2024-01-01T12:00:00Z"
    oid = OrderId("00000000-0000-4000-8000-00000000dd10")
    ev = [
        session_started(sid, "paper", ts),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("3"), OrderType.MARKET, None, ts),
        order_acknowledged(oid, ts, broker_order_id="brk-99"),
    ]
    by_c, alp, an = rehydrate_tracked_orders_from_events(ev, Decimal("100000"))
    assert not an
    assert str(oid) in by_c
    assert alp[str(oid)] == "brk-99"


def test_merge_rehydrate_idempotent_with_seen_exec() -> None:
    http = SequentialAlpacaHttp()
    http.order_pages = [[]]
    http.activity_pages = [
        [
            {
                "id": "exec-dup",
                "activity_type": "FILL",
                "order_id": "brk-99",
                "qty": "3",
                "price": "10",
                "side": "buy",
                "transaction_time": "2024-01-01T12:00:00Z",
            }
        ]
    ]
    ad = AlpacaPaperAdapter(CFG, http=http)
    sid = SessionId("00000000-0000-4000-8000-00000000dd01")
    ts = "2024-01-01T12:00:00Z"
    oid = OrderId("00000000-0000-4000-8000-00000000dd10")
    ev = [
        session_started(sid, "paper", ts),
        order_submitted(oid, IID, OrderSide.BUY, Decimal("3"), OrderType.MARKET, None, ts),
        order_acknowledged(oid, ts, broker_order_id="brk-99"),
    ]
    by_c, alp, _ = rehydrate_tracked_orders_from_events(ev, Decimal("100000"))
    ad.merge_rehydrated_tracked(by_c, alp, union_fill_emitted={"exec-dup"})
    h = BrokerPollHints(default_occurred_at_utc=ts)
    batch = ad.poll_broker_facts(cursor=None, hints=h, phase=BrokerSyncPhase.STARTUP)
    assert all(
        getattr(f, "broker_execution_id", None) != "exec-dup" for f in batch.facts
    )


def test_runtime_submit_uncertainty_unresolved_raises(tmp_path) -> None:
    from datetime import time

    from gkr_trading.core.intents.models import TradeIntent
    from gkr_trading.core.risk import RiskLimits
    from gkr_trading.core.schemas.ids import new_intent_id
    from gkr_trading.data.access_api.service import DataAccessAPI
    from gkr_trading.data.instrument_master.repository import InstrumentRepository
    from gkr_trading.data.market_store.repository import BarRow
    from gkr_trading.live.broker_symbol import make_alpaca_equity_symbol_resolver
    from gkr_trading.live.runtime import run_paper_session
    from gkr_trading.persistence.event_store import SqliteEventStore

    class UncHttp(SequentialAlpacaHttp):
        def request_json(self, method, path, *, query=None, json_body=None):
            if method == "POST" and path == "/v2/orders":
                raise AlpacaSubmitUncertaintyError("simulated", client_order_id=None)
            return super().request_json(method, path, query=query, json_body=json_body)

    class TradeOnce:
        name = "once"

        def __init__(self) -> None:
            self._done = False

        def on_bar(self, bar: BarRow, history: tuple[BarRow, ...]):
            if self._done or len(history) < 2:
                return None
            self._done = True
            return TradeIntent(
                intent_id=new_intent_id(),
                instrument_id=bar.instrument_id,
                side=OrderSide.BUY,
                quantity=Decimal("1"),
                order_type=OrderType.MARKET,
                strategy_name=self.name,
            )

    db = str(tmp_path / "u.db")
    from gkr_trading.cli import seed as seedmod

    conn = seedmod.initialize_database(db)
    seedmod.seed_instruments(conn)
    seedmod.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    repo = InstrumentRepository(conn)
    limits = RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )
    http = UncHttp()
    brk = AlpacaPaperAdapter(CFG, http=http)
    from gkr_trading.live.paper_session_report import PaperSessionRunFailed

    with pytest.raises(PaperSessionRunFailed) as ei:
        run_paper_session(
            api=api,
            store=store,
            session_id=SessionId("00000000-0000-4000-8000-00000000ee01"),
            strategy=TradeOnce(),
            universe_name="demo",
            timeframe=Timeframe.D1,
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-12-31T23:59:59Z",
            starting_cash=Decimal("100000"),
            risk_limits=limits,
            broker=brk,
            symbol_resolver=make_alpaca_equity_symbol_resolver(repo),
        )
    assert isinstance(ei.value.__cause__, AlpacaSubmitUnresolvedError)
    rep = ei.value.report
    assert rep.status == "failed"
    assert rep.uncertainty_unresolved is True
    conn.close()
