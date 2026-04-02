"""Phase 1 paper orchestrator: sync fences, ordering, dedupe, symbol resolution."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

import pytest

from gkr_trading.cli import seed
from gkr_trading.core.events.builders import session_started
from gkr_trading.core.events.types import EventType
from gkr_trading.core.intents.models import TradeIntent
from gkr_trading.core.replay.engine import replay_portfolio_state
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import OrderSide, OrderType, Timeframe
from gkr_trading.core.schemas.ids import InstrumentId, SessionId, new_intent_id
from gkr_trading.core.sessions.manager import SessionManager
from gkr_trading.data.access_api.service import DataAccessAPI
from gkr_trading.data.instrument_master.repository import InstrumentRepository
from gkr_trading.data.market_store.repository import BarRow
from gkr_trading.live.broker_adapter import (
    BrokerFillFact,
    BrokerPollHints,
    BrokerSyncPhase,
    MockBrokerAdapter,
)
from gkr_trading.live.broker_symbol import (
    InstrumentSymbolResolutionError,
    make_alpaca_equity_symbol_resolver,
)
from gkr_trading.live.broker_sync import PaperBrokerSessionContext
from gkr_trading.live.runtime import run_paper_session
from gkr_trading.persistence.event_store import SqliteEventStore


class IdleStrategy:
    name = "idle"

    def on_bar(self, bar: BarRow, history: tuple[BarRow, ...]) -> TradeIntent | None:
        return None


class BuyAfterTwoBars:
    name = "buy_after_two"

    def __init__(self, qty: Decimal = Decimal("3")) -> None:
        self._qty = qty

    def on_bar(self, bar: BarRow, history: tuple[BarRow, ...]) -> TradeIntent | None:
        if len(history) < 2:
            return None
        return TradeIntent(
            intent_id=new_intent_id(),
            instrument_id=bar.instrument_id,
            side=OrderSide.BUY,
            quantity=self._qty,
            order_type=OrderType.MARKET,
            strategy_name=self.name,
        )


class StaleTsFillMock(MockBrokerAdapter):
    """Emits a single fill with an old occurred_at_utc via post_submit queue (no auto synthetic)."""

    def __init__(self) -> None:
        super().__init__(synthetic_fill_enabled=False)
        self._stale_injected = False

    def poll_broker_facts(
        self,
        *,
        cursor,
        hints: BrokerPollHints,
        phase: BrokerSyncPhase,
    ):
        if (
            phase == BrokerSyncPhase.POST_SUBMIT
            and not self._stale_injected
            and self.submitted
        ):
            r = self.submitted[-1]
            self.inject_post_submit_fact(
                BrokerFillFact(
                    client_order_id=r.order_id,
                    instrument_id=r.instrument_id,
                    side=r.side,
                    quantity=r.quantity,
                    price=hints.reference_price or Decimal("100"),
                    fees=Decimal("0"),
                    fill_ts_utc="1999-12-31T23:59:59Z",
                    occurred_at_utc="1999-12-31T23:59:59Z",
                    broker_execution_id="stale-ts-exec",
                )
            )
            self._stale_injected = True
        return super().poll_broker_facts(cursor=cursor, hints=hints, phase=phase)


def _risk() -> RiskLimits:
    return RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )


def _session_kwargs(api: DataAccessAPI, store: SqliteEventStore, **kwargs):
    return dict(
        api=api,
        store=store,
        session_id=SessionId("00000000-0000-4000-8000-00000000cc99"),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_risk(),
    ) | kwargs


def test_pre_bar_fill_before_next_market_bar_when_deferred(tmp_path) -> None:
    """Deferred mock fill is appended at pre_bar of the following bar, before that bar's market event."""
    db = str(tmp_path / "o.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    broker = MockBrokerAdapter(defer_fill_to_next_pre_bar=True)
    run_paper_session(
        **_session_kwargs(api, store, strategy=BuyAfterTwoBars(), broker=broker),
    )
    events = store.load_session("00000000-0000-4000-8000-00000000cc99")
    conn.close()
    assert len(broker.submitted) >= 1
    mb_idx = [i for i, e in enumerate(events) if e.event_type == EventType.MARKET_DATA_RECEIVED]
    fill_idx = [i for i, e in enumerate(events) if e.event_type == EventType.FILL_RECEIVED]
    assert fill_idx, "expected fills"
    first_fill = fill_idx[0]
    assert first_fill < mb_idx[3], "pre_bar fill must precede fourth market bar"


def test_post_bar_duplicate_execution_id_skipped(tmp_path) -> None:
    db = str(tmp_path / "o3.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    broker = MockBrokerAdapter(emit_duplicate_fill_on_post_bar=True)
    run_paper_session(
        **_session_kwargs(api, store, strategy=BuyAfterTwoBars(), broker=broker),
    )
    events = store.load_session("00000000-0000-4000-8000-00000000cc99")
    conn.close()
    fills = [e for e in events if e.event_type == EventType.FILL_RECEIVED]
    exec_ids = [e.payload.broker_execution_id for e in fills if e.payload.broker_execution_id]
    assert len(exec_ids) == len(set(exec_ids)), "duplicate broker_execution_id must not double-append"


def test_seq_replay_truth_despite_old_occurred_at_payload(tmp_path) -> None:
    """Fill row may carry an older occurred_at_utc; replay still follows seq and updates positions."""
    db = str(tmp_path / "o4.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    broker = StaleTsFillMock()
    run_paper_session(
        **_session_kwargs(api, store, strategy=BuyAfterTwoBars(), broker=broker),
    )
    events = store.load_session("00000000-0000-4000-8000-00000000cc99")
    conn.close()
    fills = [e for e in events if e.event_type == EventType.FILL_RECEIVED]
    assert fills and fills[0].occurred_at_utc.startswith("1999")
    rep = replay_portfolio_state(events, Decimal("100000"))
    r = broker.submitted[0]
    assert str(r.instrument_id) in rep.state.positions


def test_symbol_resolver_failure(tmp_path) -> None:
    db = str(tmp_path / "o5.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)

    def bad(_iid: InstrumentId) -> str:
        raise InstrumentSymbolResolutionError("no symbol")

    from gkr_trading.live.paper_session_report import PaperSessionRunFailed

    with pytest.raises(PaperSessionRunFailed) as ei:
        run_paper_session(
            **_session_kwargs(
                api,
                store,
                strategy=BuyAfterTwoBars(),
                symbol_resolver=bad,
            ),
        )
    assert isinstance(ei.value.__cause__, InstrumentSymbolResolutionError)
    conn.close()


def test_symbol_resolver_maps_vendor_symbol(tmp_path) -> None:
    db = str(tmp_path / "o6.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    repo = InstrumentRepository(conn)
    resolver = make_alpaca_equity_symbol_resolver(repo)
    broker = MockBrokerAdapter()
    run_paper_session(
        **_session_kwargs(
            api,
            store,
            strategy=BuyAfterTwoBars(),
            broker=broker,
            symbol_resolver=resolver,
        ),
    )
    conn.close()
    assert broker.submitted
    assert broker.submitted[0].executable_broker_symbol == "SPY"


def test_startup_sync_runs_without_error(tmp_path) -> None:
    db = str(tmp_path / "o7.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    bctx = PaperBrokerSessionContext()
    broker = MockBrokerAdapter()
    run_paper_session(
        **_session_kwargs(
            api,
            store,
            strategy=BuyAfterTwoBars(),
            broker=broker,
            broker_session=bctx,
        ),
    )
    conn.close()
    assert bctx.reconciliation_cursor is not None
    assert bctx.reconciliation_cursor.token != ""


def test_replay_happy_path_matches_direct_fold(tmp_path) -> None:
    db = str(tmp_path / "o8.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    end = run_paper_session(
        **_session_kwargs(api, store, strategy=BuyAfterTwoBars(), broker=MockBrokerAdapter()),
    ).state
    events = store.load_session("00000000-0000-4000-8000-00000000cc99")
    conn.close()
    rep = replay_portfolio_state(events, Decimal("100000"))
    assert rep.state.positions == end.positions


def test_resume_skips_second_session_started(tmp_path) -> None:
    db = str(tmp_path / "o9.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    store = SqliteEventStore(conn)
    api = DataAccessAPI(conn)
    sid = SessionId("00000000-0000-4000-8000-00000000dd88")
    sm = SessionManager(store, sid)
    sm.append(session_started(sid, "paper", "2024-01-01T00:00:00Z"))
    bctx = PaperBrokerSessionContext()
    pr = run_paper_session(
        api=api,
        store=store,
        session_id=sid,
        strategy=IdleStrategy(),
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_risk(),
        broker=MockBrokerAdapter(),
        broker_session=bctx,
        resume_existing_session=True,
    )
    assert pr.report.resumed_session is True
    ev = store.load_session(str(sid))
    conn.close()
    assert sum(1 for e in ev if e.event_type == EventType.SESSION_STARTED) == 1
    assert isinstance(bctx.recovery.startup_broker_facts_seen, int)
