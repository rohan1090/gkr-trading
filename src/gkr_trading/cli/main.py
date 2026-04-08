from __future__ import annotations

import json
from collections import Counter
from datetime import time
from decimal import Decimal

import typer
from rich import print as rprint

from gkr_trading.core.replay import ReplayEngine
from gkr_trading.core.risk import RiskLimits
from gkr_trading.core.schemas.enums import Timeframe
from gkr_trading.core.schemas.ids import SessionId, new_session_id
from gkr_trading.backtest.orchestrator import run_backtest
from gkr_trading.cli import seed
from gkr_trading.data.access_api.service import DataAccessAPI
from typing import Literal

from gkr_trading.data.instrument_master.repository import InstrumentRepository
from gkr_trading.live.alpaca_config import AlpacaConfigError, AlpacaPaperConfig
from gkr_trading.live.alpaca_paper_adapter import AlpacaPaperAdapter
from gkr_trading.live.broker_adapter import BrokerAdapter, MockBrokerAdapter
from gkr_trading.live.broker_symbol import make_alpaca_equity_symbol_resolver
from gkr_trading.live.paper_session_report import (
    PaperSessionFailureReport,
    PaperSessionOperatorReport,
    PaperSessionRunFailed,
)
from gkr_trading.live.runtime import run_paper_session
from gkr_trading.persistence.db import open_sqlite
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.strategy.sample_strategy import SampleBarCrossStrategy

from gkr_trading.cli.commands.operator import operator_app
from gkr_trading.cli.commands.paper_v2 import _build_and_run as _paper_v2_build_and_run
from gkr_trading.cli.commands.paper_v2 import paper_v2_app as _paper_v2_sub_app

app = typer.Typer(no_args_is_help=True, help="GKR Trading V1 operator CLI")
app.add_typer(operator_app, name="operator")

AdapterChoice = Literal["mock", "alpaca", "dry_run"]


def _replay_anomaly_types_histogram(anomalies: tuple, *, top_n: int = 12) -> list[dict[str, int]]:
    ctr = Counter(a.code for a in anomalies)
    return [{"code": c, "count": n} for c, n in ctr.most_common(top_n)]


def _default_risk() -> RiskLimits:
    return RiskLimits(
        max_position_abs=Decimal("1000000"),
        max_notional_per_trade=Decimal("10000000"),
        session_start_utc=time(0, 0, 0),
        session_end_utc=time(23, 59, 59),
        kill_switch=False,
    )


@app.command("init-db")
def init_db(db_path: str = typer.Option(..., "--db-path")) -> None:
    seed.initialize_database(db_path)
    rprint(f"Initialized schema at [green]{db_path}[/green]")


@app.command("ingest-instruments")
def ingest_instruments(db_path: str = typer.Option(..., "--db-path")) -> None:
    conn = open_sqlite(db_path)
    seed.seed_instruments(conn)
    conn.close()
    rprint("Instruments and demo universe ingested.")


@app.command("ingest-bars")
def ingest_bars(db_path: str = typer.Option(..., "--db-path")) -> None:
    conn = open_sqlite(db_path)
    seed.seed_equity_bars(conn)
    conn.close()
    rprint("Sample equity bars ingested.")


@app.command("backtest")
def backtest_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str | None = typer.Option(None, "--session-id"),
) -> None:
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    sid = SessionId(session_id) if session_id else new_session_id()
    api = DataAccessAPI(conn)
    strat = SampleBarCrossStrategy(trade_qty=Decimal("10"))
    state = run_backtest(
        api=api,
        store=store,
        session_id=sid,
        strategy=strat,
        universe_name="demo",
        timeframe=Timeframe.D1,
        start_ts="2024-01-01T00:00:00Z",
        end_ts="2024-12-31T23:59:59Z",
        starting_cash=Decimal("100000"),
        risk_limits=_default_risk(),
    )
    conn.close()
    rprint(f"Session [cyan]{sid}[/cyan] complete. Cash={state.cash} realized={state.realized_pnl}")


def _paper_session_common(
    *,
    db_path: str,
    session_id: str | None,
    dry_run: bool,
    resume_existing_session: bool,
    adapter: AdapterChoice | None,
) -> tuple[SessionId, PaperSessionOperatorReport | PaperSessionFailureReport, int]:
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    sid = SessionId(session_id) if session_id else new_session_id()
    api = DataAccessAPI(conn)
    strat = SampleBarCrossStrategy(trade_qty=Decimal("10"))
    choice: AdapterChoice
    if adapter is not None:
        choice = adapter
    else:
        # Auto-detect for paper runs: env present => alpaca; else => mock.
        try:
            AlpacaPaperConfig.from_env()
            choice = "alpaca"
        except AlpacaConfigError:
            choice = "mock"

    broker: BrokerAdapter
    symbol_resolver = None
    effective_dry_run = dry_run
    n_submits: int = 0

    if choice == "dry_run":
        broker = MockBrokerAdapter()
        effective_dry_run = True
    elif choice == "mock":
        broker = MockBrokerAdapter()
    else:
        # choice == "alpaca"
        cfg = AlpacaPaperConfig.from_env()
        broker = AlpacaPaperAdapter(cfg)
        repo = InstrumentRepository(conn)
        symbol_resolver = make_alpaca_equity_symbol_resolver(repo)
    try:
        result = run_paper_session(
            api=api,
            store=store,
            session_id=sid,
            strategy=strat,
            universe_name="demo",
            timeframe=Timeframe.D1,
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-12-31T23:59:59Z",
            starting_cash=Decimal("100000"),
            risk_limits=_default_risk(),
            broker=broker,
            symbol_resolver=symbol_resolver,
            resume_existing_session=resume_existing_session,
            dry_run=effective_dry_run,
        )
        n_submits = len(getattr(broker, "submitted", []))
        report = result.report
        conn.close()
        return sid, report, n_submits
    except PaperSessionRunFailed as e:
        n_submits = len(getattr(broker, "submitted", []))
        conn.close()
        return sid, e.report, n_submits


@app.command("paper")
def paper_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str | None = typer.Option(None, "--session-id"),
    adapter: AdapterChoice | None = typer.Option(
        None,
        "--adapter",
        help="Override adapter selection (mock|alpaca|dry_run). Default: auto-detect from env.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        help="Single-line summary only (legacy automation).",
    ),
    as_json: bool = typer.Option(
        False,
        "--json",
        help="Print operator report as JSON only (stable keys).",
    ),
    resume_existing_session: bool = typer.Option(
        False,
        "--resume-existing-session",
        help="If the log already has SESSION_STARTED for this session_id, do not append another.",
    ),
) -> None:
    sid, report, n_submits = _paper_session_common(
        db_path=db_path,
        session_id=session_id,
        dry_run=False,
        resume_existing_session=resume_existing_session,
        adapter=adapter,
    )
    if quiet:
        if isinstance(report, PaperSessionFailureReport):
            rprint(
                f"[red]Paper session FAILED[/red] [cyan]{sid}[/cyan]. "
                f"failure_type={report.failure_type} failure_phase={report.failure_phase}"
            )
            raise typer.Exit(code=1)
        rprint(f"Paper session [cyan]{sid}[/cyan]. Submits={n_submits} cash={report.final_cash}")
        return
    if as_json:
        rprint(json.dumps(report.to_jsonable(), indent=2))
        if isinstance(report, PaperSessionFailureReport):
            raise typer.Exit(code=1)
        return
    rprint(f"[bold]Paper session[/bold] [cyan]{sid}[/cyan]")
    rprint(json.dumps(report.to_jsonable(), indent=2))
    if isinstance(report, PaperSessionFailureReport):
        raise typer.Exit(code=1)


@app.command("paper-dry-run")
def paper_dry_run_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str | None = typer.Option(
        None,
        "--session-id",
        help="Fixed id for repeatable reports/tests; random if omitted.",
    ),
    adapter: AdapterChoice = typer.Option(
        "dry_run",
        "--adapter",
        help="Override adapter selection (mock|alpaca|dry_run). Default: dry_run.",
    ),
    quiet: bool = typer.Option(False, "--quiet", help="Single-line summary only."),
    as_json: bool = typer.Option(False, "--json", help="Print operator report as JSON only."),
) -> None:
    """Run the paper orchestration path with a mock broker (no Alpaca, no network)."""
    sid, report, n_submits = _paper_session_common(
        db_path=db_path,
        session_id=session_id,
        dry_run=True,
        resume_existing_session=False,
        adapter=adapter,
    )
    if quiet:
        if isinstance(report, PaperSessionFailureReport):
            rprint(
                f"[red]Paper dry-run FAILED[/red] [cyan]{sid}[/cyan]. "
                f"failure_type={report.failure_type} failure_phase={report.failure_phase}"
            )
            raise typer.Exit(code=1)
        rprint(
            f"Paper dry-run [cyan]{sid}[/cyan]. Submits={n_submits} cash={report.final_cash} "
            f"adapter_mode={report.adapter_mode}"
        )
        return
    if as_json:
        rprint(json.dumps(report.to_jsonable(), indent=2))
        if isinstance(report, PaperSessionFailureReport):
            raise typer.Exit(code=1)
        return
    rprint(f"[bold]Paper session dry-run[/bold] [cyan]{sid}[/cyan] (mock broker, no network)")
    rprint(json.dumps(report.to_jsonable(), indent=2))
    if isinstance(report, PaperSessionFailureReport):
        raise typer.Exit(code=1)


@app.command("session-inspect")
def session_inspect(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    starting_cash: str = typer.Option("100000", "--starting-cash"),
) -> None:
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    evs = store.load_session(session_id)
    counts: dict[str, int] = {}
    for e in evs:
        counts[e.event_type.value] = counts.get(e.event_type.value, 0) + 1
    rejects_preview = []
    for e in evs:
        if e.event_type.value != "order_rejected":
            continue
        rejects_preview.append(
            {
                "order_id": str(getattr(e.payload, "order_id", "")),
                "reason_code": getattr(e.payload, "reason_code", None),
                "reason_detail": getattr(e.payload, "reason_detail", None),
            }
        )
        if len(rejects_preview) >= 10:
            break
    eng = ReplayEngine(store, Decimal(starting_cash))
    replay_res, _ = eng.replay_session(SessionId(session_id))
    conn.close()
    preview = [
        {"code": a.code, "message": a.message, "event_index": a.event_index}
        for a in replay_res.anomalies[:20]
    ]
    anomaly_types = _replay_anomaly_types_histogram(replay_res.anomalies)
    # Options lifecycle summary
    options_lifecycle = {
        "assignments": counts.get("assignment_received", 0),
        "exercises": counts.get("exercise_processed", 0),
        "expirations": counts.get("expiration_processed", 0),
        "operator_commands": counts.get("operator_command", 0),
        "reconciliation_completed": counts.get("reconciliation_completed", 0),
    }
    rprint(
        json.dumps(
            {
                "count": len(evs),
                "by_type": counts,
                "options_lifecycle": options_lifecycle,
                "order_rejects_preview": rejects_preview,
                "replay_anomaly_count": len(replay_res.anomalies),
                "replay_anomaly_types": anomaly_types,
                "replay_anomalies_preview": preview,
            },
            indent=2,
        )
    )


@app.command("replay")
def replay_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    starting_cash: str = typer.Option("100000", "--starting-cash"),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Audit mode: fail replay on first invariant violation (reconciliation).",
    ),
) -> None:
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    eng = ReplayEngine(store, Decimal(starting_cash))
    try:
        result, events = eng.replay_session(SessionId(session_id), strict=strict)
    except Exception as e:
        conn.close()
        rprint(f"[red]Replay failed:[/red] {e!s}")
        raise typer.Exit(code=1) from e
    state = result.state
    conn.close()
    summary = {
        "cash": str(state.cash),
        "positions": {k: str(v) for k, v in state.positions.items()},
        "realized_pnl": str(state.realized_pnl),
        "unrealized_pnl": str(state.unrealized_pnl),
        "fills": len(state.fill_history),
        "events_replayed": len(events),
        "anomaly_count": len(result.anomalies),
        "anomalies": [
            {
                "code": a.code,
                "message": a.message,
                "event_type": a.event_type,
                "event_index": a.event_index,
            }
            for a in result.anomalies
        ],
    }
    rprint(json.dumps(summary, indent=2))


@app.command("portfolio-show")
def portfolio_show(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    starting_cash: str = typer.Option("100000", "--starting-cash"),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Audit mode: fail on first invariant violation.",
    ),
) -> None:
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    eng = ReplayEngine(store, Decimal(starting_cash))
    try:
        result, events = eng.replay_session(SessionId(session_id), strict=strict)
    except Exception as e:
        conn.close()
        rprint(f"[red]Replay failed:[/red] {e!s}")
        raise typer.Exit(code=1) from e
    state = result.state
    conn.close()
    rprint(
        json.dumps(
            {
                "cash": str(state.cash),
                "positions": {k: str(v) for k, v in state.positions.items()},
                "realized_pnl": str(state.realized_pnl),
                "unrealized_pnl": str(state.unrealized_pnl),
                "fills": len(state.fill_history),
                "events_replayed": len(events),
                "anomaly_count": len(result.anomalies),
                "anomalies": [
                    {
                        "code": a.code,
                        "message": a.message,
                        "event_type": a.event_type,
                        "event_index": a.event_index,
                    }
                    for a in result.anomalies
                ],
            },
            indent=2,
        )
    )


# ---------------------------------------------------------------------------
# V2 paper runtime commands
# ---------------------------------------------------------------------------

PaperV2AdapterMode = Literal["mock", "alpaca"]
PaperV2StrategyChoice = Literal["equity", "options"]


@app.command("paper-v2")
def paper_v2_cmd(
    db_path: str = typer.Option(..., "--db-path", help="Path to SQLite database."),
    session_id: str | None = typer.Option(
        None, "--session-id", help="Fixed session ID. Random if omitted.",
    ),
    adapter: PaperV2AdapterMode = typer.Option(
        "mock", "--adapter", help="Adapter: mock (no network) or alpaca (paper API).",
    ),
    strategy: PaperV2StrategyChoice = typer.Option(
        "equity", "--strategy", help="Sample strategy: equity or options.",
    ),
    shadow: bool = typer.Option(
        False, "--shadow", help="Shadow mode: log intents but do not submit orders.",
    ),
    risk_config: str | None = typer.Option(
        None, "--risk-config", help="Path to risk policy YAML.",
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Output as JSON only.",
    ),
) -> None:
    """Run an end-to-end V2 paper session (new architecture)."""
    try:
        result = _paper_v2_build_and_run(
            db_path=db_path,
            session_id=session_id,
            adapter_mode=adapter,
            strategy_choice=strategy,
            shadow_mode=shadow,
            risk_config_path=risk_config,
        )
    except Exception as e:
        rprint(f"[red]Session error:[/red] {e}")
        raise typer.Exit(code=1) from e

    if as_json:
        rprint(json.dumps(result, indent=2, default=str))
    else:
        status = result.get("status", "unknown")
        sid_out = result.get("session_id", "?")
        if status != "ok":
            rprint(f"[red]Paper V2 session FAILED[/red] [cyan]{sid_out}[/cyan]")
            rprint(json.dumps(result, indent=2, default=str))
            raise typer.Exit(code=1)
        rprint(f"[bold]Paper V2 session[/bold] [cyan]{sid_out}[/cyan]")
        rprint(f"  adapter={result.get('adapter_mode')}  strategy={result.get('strategy')}")
        rprint(f"  shadow={result.get('shadow_mode')}  startup_clean={result.get('startup_clean')}")
        rprint(f"  shutdown_clean={result.get('shutdown_clean')}")
        rprint(f"  intents={result.get('intents_generated')}  approved={result.get('intents_approved')}")
        rprint(f"  submitted={result.get('orders_submitted')}  fills={result.get('fills_count')}")
        rprint(f"  events={result.get('events_count')}  errors={result.get('errors')}")
    if result.get("status") != "ok":
        raise typer.Exit(code=1)


@app.command("paper-v2-continuous")
def paper_v2_continuous_cmd(
    db_path: str = typer.Option(..., "--db-path", help="Path to SQLite database."),
    session_id: str | None = typer.Option(
        None, "--session-id", help="Fixed session ID. Random if omitted.",
    ),
    strategy: PaperV2StrategyChoice = typer.Option(
        "equity", "--strategy", help="Sample strategy: equity or options.",
    ),
    shadow: bool = typer.Option(
        False, "--shadow", help="Shadow mode.",
    ),
    poll_interval: float = typer.Option(
        15.0, "--poll-interval", help="Market data poll interval in seconds.",
    ),
    max_cycles: int | None = typer.Option(
        None, "--max-cycles", help="Max poll cycles. None=run until close.",
    ),
    no_websocket: bool = typer.Option(
        False, "--no-websocket", help="Disable WebSocket.",
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Output as JSON only.",
    ),
) -> None:
    """Run a continuous V2 paper session with real market data."""
    from gkr_trading.cli.commands.paper_v2 import _run_continuous_session
    _run_continuous_session(
        db_path=db_path,
        session_id=session_id,
        strategy=strategy,
        shadow=shadow,
        risk_config=None,
        poll_interval=poll_interval,
        max_cycles=max_cycles,
        no_websocket=no_websocket,
        as_json=as_json,
    )


@app.command("paper-v2-certify")
def paper_v2_certify_cmd(
    db_path: str = typer.Option(..., "--db-path", help="Path to SQLite database."),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON only."),
) -> None:
    """Run both equity and options sample strategies end-to-end (mock adapter)."""
    results = []
    for strat in ("equity", "options"):
        result = _paper_v2_build_and_run(
            db_path=db_path,
            session_id=None,
            adapter_mode="mock",
            strategy_choice=strat,
            shadow_mode=False,
            risk_config_path=None,
        )
        results.append(result)

    all_ok = all(r.get("status") == "ok" for r in results)

    if as_json:
        rprint(json.dumps({"passed": all_ok, "sessions": results}, indent=2, default=str))
    else:
        for r in results:
            tag = "[green]PASS[/green]" if r.get("status") == "ok" else "[red]FAIL[/red]"
            rprint(f"  {tag} {r.get('strategy')} session={r.get('session_id')}")

        if all_ok:
            rprint("[bold green]Paper certification PASSED[/bold green]")
        else:
            rprint("[bold red]Paper certification FAILED[/bold red]")
            raise typer.Exit(code=1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
