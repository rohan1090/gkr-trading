"""CLI commands for V2 paper runtime (new architecture).

`gkr paper-v2` — Run an end-to-end Alpaca paper session through the new
control-plane path:

    Strategy → TradeIntent → RiskApprovalGate → OrderSubmissionService
    → Alpaca adapters → fill/NTA translation → position accounting
    → reconciliation → replay → operator controls
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict

import typer
from rich import print as rprint
from typing import Literal, Optional

from gkr_trading.live.alpaca_config import AlpacaConfigError, AlpacaPaperConfig
from gkr_trading.persistence.db import open_sqlite

paper_v2_app = typer.Typer(no_args_is_help=True, help="V2 paper runtime commands")

AdapterMode = Literal["mock", "alpaca"]
StrategyChoice = Literal["equity", "options"]


def _build_and_run(
    *,
    db_path: str,
    session_id: Optional[str],
    adapter_mode: AdapterMode,
    strategy_choice: StrategyChoice,
    shadow_mode: bool,
    risk_config_path: Optional[str],
) -> dict:
    """Shared logic: wire V2 runtime, run one market-data cycle, return result dict."""
    import uuid
    import time

    from gkr_trading.live.runtime_v2 import build_paper_runner
    from gkr_trading.core.instruments import EquityRef, OptionsRef
    from gkr_trading.core.market_data import MarketDataEnvelope

    conn = open_sqlite(db_path)
    sid = session_id or str(uuid.uuid4())

    # --- Adapter ---
    if adapter_mode == "alpaca":
        cfg = AlpacaPaperConfig.from_env()
        from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient
        from gkr_trading.live.traditional.alpaca.alpaca_adapter import (
            AlpacaPaperEquityAdapter,
        )
        from gkr_trading.live.traditional.alpaca.alpaca_options_adapter import (
            AlpacaOptionsAdapter,
        )
        from gkr_trading.live.traditional.alpaca.alpaca_fill_translator import (
            AlpacaFillTranslator,
        )
        from gkr_trading.live.traditional.alpaca.alpaca_options_fill_translator import (
            AlpacaOptionsFillTranslator,
        )

        http = UrllibAlpacaHttpClient(config=cfg)
        equity_adapter = AlpacaPaperEquityAdapter(http)
        options_adapter = AlpacaOptionsAdapter(http, session_id=sid)
        equity_fill_translator = AlpacaFillTranslator(session_id=sid)
        options_fill_translator = AlpacaOptionsFillTranslator(session_id=sid)
    else:
        # Mock mode — no network
        from gkr_trading.live.base import (
            VenueAdapter,
            VenuePosition,
            VenueAccountInfo,
            SubmissionRequest,
            SubmissionResponse,
        )
        from gkr_trading.core.order_model import OrderStatus
        from gkr_trading.core.fills import FillEvent
        from gkr_trading.live.fill_translator import FillTranslator

        class MockEquityAdapter(VenueAdapter):
            def __init__(self):
                self._submitted = []
                self._positions: list[VenuePosition] = []

            @property
            def venue_name(self) -> str:
                return "mock_paper"

            def submit_order(self, request: SubmissionRequest) -> SubmissionResponse:
                self._submitted.append(request)
                return SubmissionResponse(
                    client_order_id=request.client_order_id,
                    venue_order_id=f"mock-{request.client_order_id[:8]}",
                    success=True,
                )

            def cancel_order(self, coid: str) -> bool:
                return True

            def get_order_status(self, coid: str) -> Optional[OrderStatus]:
                return OrderStatus.FILLED

            def get_positions(self) -> list[VenuePosition]:
                return self._positions

            def get_account(self) -> VenueAccountInfo:
                return VenueAccountInfo(
                    cash_cents=10_000_000, buying_power_cents=10_000_000,
                )

        class MockFillXlator(FillTranslator):
            def translate_fill(self, payload: dict) -> Optional[FillEvent]:
                return None

        equity_adapter = MockEquityAdapter()
        options_adapter = None
        equity_fill_translator = MockFillXlator()
        options_fill_translator = None

    # --- Risk gates ---
    risk_gates = []
    if risk_config_path:
        from gkr_trading.live.traditional.options.options_risk_policy import (
            OptionsRiskPolicy,
        )
        policy = OptionsRiskPolicy.from_config(risk_config_path)
        risk_gates.append(policy)

    # --- Strategy ---
    if strategy_choice == "equity":
        from gkr_trading.strategy.sample_equity_v2 import SampleEquityStrategyV2
        strategy = SampleEquityStrategyV2(session_id=sid, quantity=10)
    else:
        from gkr_trading.strategy.sample_options_v2 import SampleLongCallStrategyV2
        strategy = SampleLongCallStrategyV2(session_id=sid, quantity=1)

    # --- Build runner ---
    runner = build_paper_runner(
        conn=conn,
        session_id=sid,
        equity_adapter=equity_adapter,
        options_adapter=options_adapter,
        equity_fill_translator=equity_fill_translator,
        options_fill_translator=options_fill_translator,
        risk_gates=risk_gates if risk_gates else None,
        strategy=strategy,
        shadow_mode=shadow_mode,
        venue="alpaca_paper" if adapter_mode == "alpaca" else "mock_paper",
    )

    # --- Run ---
    ok = runner.startup()
    if not ok:
        conn.close()
        return {"status": "startup_failed", "session_id": sid}

    # Feed one synthetic envelope for certification
    if strategy_choice == "equity":
        envelope = MarketDataEnvelope(
            instrument_ref=EquityRef(ticker="AAPL"),
            timestamp_ns=time.time_ns(),
            close_cents=15000,
            last_cents=15000,
        )
        # Second envelope to trigger dip-buy
        envelope2 = MarketDataEnvelope(
            instrument_ref=EquityRef(ticker="AAPL"),
            timestamp_ns=time.time_ns(),
            close_cents=14900,
            last_cents=14900,
        )
        runner.process_market_data(envelope)
        outcome = runner.process_market_data(envelope2)
    else:
        # Options — feed a call option envelope
        from datetime import date
        ref = OptionsRef(
            underlying="AAPL",
            expiry=date(2026, 6, 19),
            strike_cents=15000,
            right="call",
            multiplier=100,
            occ_symbol="AAPL260619C00150000",
        )
        envelope = MarketDataEnvelope(
            instrument_ref=ref,
            timestamp_ns=time.time_ns(),
            close_cents=500,
            ask_cents=520,
            last_cents=510,
        )
        outcome = runner.process_market_data(envelope)

    result = runner.shutdown()
    conn.close()

    return {
        "status": "ok",
        "session_id": sid,
        "adapter_mode": adapter_mode,
        "strategy": strategy_choice,
        "shadow_mode": shadow_mode,
        **asdict(result),
    }


def _run_continuous_session(
    *,
    db_path: str,
    session_id: Optional[str],
    strategy: StrategyChoice,
    shadow: bool,
    risk_config: Optional[str],
    poll_interval: float,
    max_cycles: Optional[int],
    no_websocket: bool,
    as_json: bool,
) -> None:
    """Plain-Python implementation of the continuous session loop.

    Extracted from paper_v2_continuous so it can be called from other
    Typer commands (e.g. paper_v2_continuous_cmd in main.py) without
    needing a Typer/Click context object.
    """
    import uuid as _uuid
    import time as _time

    from gkr_trading.live.runtime_v2 import (
        ContinuousSessionRunner,
        ContinuousSessionConfig,
        build_paper_runner,
    )
    from gkr_trading.live.market_data_feed import (
        AlpacaMarketDataFeed,
        MarketDataFeedConfig,
    )
    from gkr_trading.live.market_metadata_provider import AlpacaMarketMetadataProvider

    try:
        cfg = AlpacaPaperConfig.from_env()
    except AlpacaConfigError as e:
        rprint(f"[red]Alpaca config error:[/red] {e}")
        raise typer.Exit(code=1) from e

    from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient
    from gkr_trading.live.traditional.alpaca.alpaca_adapter import AlpacaPaperEquityAdapter
    from gkr_trading.live.traditional.alpaca.alpaca_options_adapter import AlpacaOptionsAdapter
    from gkr_trading.live.traditional.alpaca.alpaca_fill_translator import AlpacaFillTranslator
    from gkr_trading.live.traditional.alpaca.alpaca_options_fill_translator import AlpacaOptionsFillTranslator

    conn = open_sqlite(db_path)
    sid = session_id or str(_uuid.uuid4())

    http = UrllibAlpacaHttpClient(config=cfg)
    equity_adapter = AlpacaPaperEquityAdapter(http)
    options_adapter = AlpacaOptionsAdapter(http, session_id=sid)
    equity_fill_translator = AlpacaFillTranslator(session_id=sid)
    options_fill_translator = AlpacaOptionsFillTranslator(session_id=sid)

    # Risk gates
    risk_gates = []
    if risk_config:
        from gkr_trading.live.traditional.options.options_risk_policy import OptionsRiskPolicy
        policy = OptionsRiskPolicy.from_config(risk_config)
        risk_gates.append(policy)

    # Strategy
    if strategy == "equity":
        from gkr_trading.strategy.sample_equity_multicycle_v2 import MultiCycleEquityStrategyV2
        strat = MultiCycleEquityStrategyV2(session_id=sid, quantity=10)
    else:
        from gkr_trading.strategy.sample_options_v2 import SampleLongCallStrategyV2
        strat = SampleLongCallStrategyV2(session_id=sid, quantity=1)

    runner = build_paper_runner(
        conn=conn,
        session_id=sid,
        equity_adapter=equity_adapter,
        options_adapter=options_adapter,
        equity_fill_translator=equity_fill_translator,
        options_fill_translator=options_fill_translator,
        risk_gates=risk_gates if risk_gates else None,
        strategy=strat,
        shadow_mode=shadow,
        venue="alpaca_paper",
    )

    # Market data feed
    md_config = MarketDataFeedConfig(
        equity_tickers=["AAPL", "SPY"] if strategy == "equity" else [],
        poll_interval_sec=poll_interval,
    )
    md_feed = AlpacaMarketDataFeed(http_client=http, config=md_config)

    # Metadata provider
    metadata = AlpacaMarketMetadataProvider(http)

    # WebSocket manager (optional)
    ws_manager = None
    if not no_websocket:
        try:
            from gkr_trading.live.websocket_manager import AlpacaWebSocketManager
            ws_manager = AlpacaWebSocketManager(
                api_key=cfg.api_key,
                secret_key=cfg.secret_key,
            )
        except Exception as exc:
            rprint(f"[yellow]WebSocket init failed (continuing without):[/yellow] {exc}")

    # Continuous session config
    cs_config = ContinuousSessionConfig(
        poll_interval_sec=poll_interval,
        max_cycles=max_cycles,
        enable_websocket=ws_manager is not None,
    )

    rprint(f"[bold]Starting continuous V2 session[/bold] [cyan]{sid}[/cyan]")
    rprint(f"  strategy={strategy}  shadow={shadow}  poll={poll_interval}s  ws={ws_manager is not None}")

    continuous = ContinuousSessionRunner(
        runner=runner,
        market_data_feed=md_feed,
        metadata_provider=metadata,
        ws_manager=ws_manager,
        config=cs_config,
    )

    try:
        result = continuous.run_session()
    except KeyboardInterrupt:
        rprint("\n[yellow]Interrupted — shutting down...[/yellow]")
        continuous.request_stop("keyboard_interrupt")
        result = continuous.run_session()
    except Exception as e:
        rprint(f"[red]Session error:[/red] {e}")
        conn.close()
        raise typer.Exit(code=1) from e

    conn.close()

    summary = {
        "status": "ok",
        "session_id": sid,
        "stop_reason": result.stop_reason,
        "cycles_completed": result.cycles_completed,
        "md_polls": result.md_polls,
        "md_envelopes": result.md_envelopes,
        "ws_connected": result.ws_connected,
        "ws_trade_updates": result.ws_trade_updates,
        "replay_anomaly_count": result.replay_anomaly_count,
        "intents_generated": result.session_result.intents_generated,
        "orders_submitted": result.session_result.orders_submitted,
        "fills": result.session_result.fills_count,
        "errors": result.session_result.errors,
    }

    if as_json:
        rprint(json.dumps(summary, indent=2, default=str))
    else:
        rprint(f"\n[bold]Continuous session complete[/bold]")
        rprint(f"  stop_reason={result.stop_reason}  cycles={result.cycles_completed}")
        rprint(f"  md_polls={result.md_polls}  md_envelopes={result.md_envelopes}")
        rprint(f"  ws={result.ws_connected}  ws_updates={result.ws_trade_updates}")
        rprint(f"  intents={result.session_result.intents_generated}  submitted={result.session_result.orders_submitted}")
        rprint(f"  fills={result.session_result.fills_count}  replay_anomalies={result.replay_anomaly_count}")
        if result.session_result.errors:
            rprint(f"  [red]errors={result.session_result.errors}[/red]")


@paper_v2_app.command("run")
def paper_v2_run(
    db_path: str = typer.Option(..., "--db-path", help="Path to SQLite database."),
    session_id: Optional[str] = typer.Option(
        None, "--session-id", help="Fixed session ID. Random if omitted.",
    ),
    adapter: AdapterMode = typer.Option(
        "mock", "--adapter", help="Adapter: mock (no network) or alpaca (paper API).",
    ),
    strategy: StrategyChoice = typer.Option(
        "equity", "--strategy", help="Sample strategy: equity or options.",
    ),
    shadow: bool = typer.Option(
        False, "--shadow", help="Shadow mode: log intents but do not submit orders.",
    ),
    risk_config: Optional[str] = typer.Option(
        None, "--risk-config", help="Path to risk policy YAML.",
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Output as JSON only.",
    ),
) -> None:
    """Run an end-to-end V2 paper session."""
    try:
        result = _build_and_run(
            db_path=db_path,
            session_id=session_id,
            adapter_mode=adapter,
            strategy_choice=strategy,
            shadow_mode=shadow,
            risk_config_path=risk_config,
        )
    except AlpacaConfigError as e:
        rprint(f"[red]Alpaca config error:[/red] {e}")
        raise typer.Exit(code=1) from e
    except Exception as e:
        rprint(f"[red]Session error:[/red] {e}")
        raise typer.Exit(code=1) from e

    if as_json:
        rprint(json.dumps(result, indent=2, default=str))
    else:
        status = result.get("status", "unknown")
        sid = result.get("session_id", "?")
        if status != "ok":
            rprint(f"[red]Paper V2 session FAILED[/red] [cyan]{sid}[/cyan]")
            rprint(json.dumps(result, indent=2, default=str))
            raise typer.Exit(code=1)
        rprint(f"[bold]Paper V2 session[/bold] [cyan]{sid}[/cyan]")
        rprint(f"  adapter={result.get('adapter_mode')}  strategy={result.get('strategy')}")
        rprint(f"  shadow={result.get('shadow_mode')}  startup_clean={result.get('startup_clean')}")
        rprint(f"  shutdown_clean={result.get('shutdown_clean')}")
        rprint(f"  intents={result.get('intents_generated')}  approved={result.get('intents_approved')}")
        rprint(f"  submitted={result.get('orders_submitted')}  fills={result.get('fills_count')}")
        rprint(f"  events={result.get('events_count')}  errors={result.get('errors')}")
    if result.get("status") != "ok":
        raise typer.Exit(code=1)


@paper_v2_app.command("continuous")
def paper_v2_continuous(
    db_path: str = typer.Option(..., "--db-path", help="Path to SQLite database."),
    session_id: Optional[str] = typer.Option(
        None, "--session-id", help="Fixed session ID. Random if omitted.",
    ),
    strategy: StrategyChoice = typer.Option(
        "equity", "--strategy", help="Sample strategy: equity or options.",
    ),
    shadow: bool = typer.Option(
        False, "--shadow", help="Shadow mode: log intents but do not submit orders.",
    ),
    risk_config: Optional[str] = typer.Option(
        None, "--risk-config", help="Path to risk policy YAML.",
    ),
    poll_interval: float = typer.Option(
        15.0, "--poll-interval", help="Market data poll interval in seconds.",
    ),
    max_cycles: Optional[int] = typer.Option(
        None, "--max-cycles", help="Max poll cycles before stopping. None=run until close.",
    ),
    no_websocket: bool = typer.Option(
        False, "--no-websocket", help="Disable WebSocket for fill streaming.",
    ),
    as_json: bool = typer.Option(
        False, "--json", help="Output as JSON only.",
    ),
) -> None:
    """Run a continuous V2 paper session with real market data."""
    _run_continuous_session(
        db_path=db_path,
        session_id=session_id,
        strategy=strategy,
        shadow=shadow,
        risk_config=risk_config,
        poll_interval=poll_interval,
        max_cycles=max_cycles,
        no_websocket=no_websocket,
        as_json=as_json,
    )


@paper_v2_app.command("certify")
def paper_v2_certify(
    db_path: str = typer.Option(..., "--db-path", help="Path to SQLite database."),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON only."),
) -> None:
    """Run both equity and options sample strategies end-to-end (mock adapter)."""
    results = []
    for strat in ("equity", "options"):
        result = _build_and_run(
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
