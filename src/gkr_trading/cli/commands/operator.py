"""Operator CLI commands — kill switch, options positions, reconciliation, status.

All state-changing commands emit OperatorCommandEvent to EventStore before execution.
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Optional

import typer
from rich import print as rprint
from rich.table import Table

from gkr_trading.core.events.envelope import CanonicalEvent
from gkr_trading.core.events.payloads import OperatorCommandPayload
from gkr_trading.core.events.types import EventType
from gkr_trading.core.operator_controls import KillSwitchLevel
from gkr_trading.persistence.db import open_sqlite
from gkr_trading.persistence.event_store import SqliteEventStore
from gkr_trading.persistence.pending_order_registry import PendingOrderRegistry
from gkr_trading.persistence.position_store import PositionStore

operator_app = typer.Typer(
    name="operator",
    help="Operator controls: kill switch, reconciliation, positions, status.",
    no_args_is_help=True,
)


def _now_utc_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# kill-switch
# ---------------------------------------------------------------------------

@operator_app.command("kill-switch")
def kill_switch_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    level: str = typer.Option(
        ...,
        "--level",
        help="Kill switch level: none | close_only | full_halt",
    ),
    reason: str = typer.Option("operator_manual", "--reason"),
    as_json: bool = typer.Option(False, "--json", help="JSON output only."),
) -> None:
    """Activate a kill-switch level for a session.

    Level 1 (close_only): Only closing orders allowed.
    Level 2 (full_halt): No orders at all.
    Level 0 (none): Reset kill switch.

    The OperatorCommandEvent is persisted BEFORE the switch takes effect.
    """
    # Validate level
    level_map = {
        "none": KillSwitchLevel.NONE,
        "close_only": KillSwitchLevel.CLOSE_ONLY,
        "full_halt": KillSwitchLevel.FULL_HALT,
        "0": KillSwitchLevel.NONE,
        "1": KillSwitchLevel.CLOSE_ONLY,
        "2": KillSwitchLevel.FULL_HALT,
        "3": KillSwitchLevel.FULL_HALT,  # L3 maps to FULL_HALT; cancel-all is separate
    }
    ks = level_map.get(level.lower())
    if ks is None:
        rprint(f"[red]Invalid level:[/red] {level}. Must be: none, close_only, full_halt, 0, 1, 2, 3")
        raise typer.Exit(code=1)

    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)

    # Persist OperatorCommandEvent BEFORE execution
    command_id = str(uuid.uuid4())
    cmd_event = CanonicalEvent(
        event_type=EventType.OPERATOR_COMMAND,
        occurred_at_utc=_now_utc_iso(),
        payload=OperatorCommandPayload(
            command_id=command_id,
            command_type="kill_switch",
            parameters=json.dumps({"level": ks.value, "reason": reason}),
            operator_id="cli",
        ),
    )
    store.append(session_id, cmd_event)

    result = {
        "action": "kill_switch_activated",
        "session_id": session_id,
        "level": ks.value,
        "reason": reason,
        "command_id": command_id,
        "persisted": True,
    }

    conn.close()

    if as_json:
        rprint(json.dumps(result, indent=2))
    else:
        color = {"none": "green", "close_only": "yellow", "full_halt": "red"}[ks.value]
        rprint(
            f"Kill switch [{color}]{ks.value.upper()}[/{color}] activated for "
            f"session [cyan]{session_id}[/cyan]. Command persisted: {command_id[:8]}..."
        )


# ---------------------------------------------------------------------------
# options-positions
# ---------------------------------------------------------------------------

@operator_app.command("options-positions")
def options_positions_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    venue: str = typer.Option("alpaca", "--venue"),
    as_json: bool = typer.Option(False, "--json", help="JSON output only."),
    include_closed: bool = typer.Option(False, "--include-closed"),
) -> None:
    """Show options positions for a session."""
    conn = open_sqlite(db_path)
    pos_store = PositionStore(conn)
    positions = pos_store.get_options_positions(session_id, venue)
    conn.close()

    if not include_closed:
        positions = [p for p in positions if p["status"] not in ("expired", "closed")]

    if as_json:
        rprint(json.dumps(positions, indent=2))
        return

    if not positions:
        rprint(f"No options positions for session [cyan]{session_id}[/cyan] @ {venue}")
        return

    table = Table(title=f"Options Positions — {session_id[:12]}... @ {venue}")
    table.add_column("OCC Symbol", style="cyan")
    table.add_column("Long", justify="right")
    table.add_column("Short", justify="right")
    table.add_column("Long Premium", justify="right")
    table.add_column("Short Premium", justify="right")
    table.add_column("P&L", justify="right")
    table.add_column("Status")
    table.add_column("Undef. Risk")

    for p in positions:
        pnl_cents = p["realized_pnl_cents"]
        pnl_str = f"${pnl_cents / 100:.2f}" if pnl_cents else "$0.00"
        long_prem = f"${p['long_premium_paid_cents'] / 100:.2f}"
        short_prem = f"${p['short_premium_received_cents'] / 100:.2f}"
        risk_indicator = "[red]YES[/red]" if p["has_undefined_risk"] else "no"

        table.add_row(
            p["occ_symbol"],
            str(p["long_contracts"]),
            str(p["short_contracts"]),
            long_prem,
            short_prem,
            pnl_str,
            p["status"],
            risk_indicator,
        )

    rprint(table)


# ---------------------------------------------------------------------------
# equity-positions
# ---------------------------------------------------------------------------

@operator_app.command("equity-positions")
def equity_positions_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    venue: str = typer.Option("alpaca", "--venue"),
    as_json: bool = typer.Option(False, "--json", help="JSON output only."),
    include_closed: bool = typer.Option(False, "--include-closed"),
) -> None:
    """Show equity positions for a session."""
    conn = open_sqlite(db_path)
    pos_store = PositionStore(conn)
    positions = pos_store.get_equity_positions(session_id, venue)
    conn.close()

    if not include_closed:
        positions = [p for p in positions if p["status"] not in ("closed",)]

    if as_json:
        rprint(json.dumps(positions, indent=2))
        return

    if not positions:
        rprint(f"No equity positions for session [cyan]{session_id}[/cyan] @ {venue}")
        return

    table = Table(title=f"Equity Positions — {session_id[:12]}... @ {venue}")
    table.add_column("Ticker", style="cyan")
    table.add_column("Qty", justify="right")
    table.add_column("Cost Basis", justify="right")
    table.add_column("Realized P&L", justify="right")
    table.add_column("Status")

    for p in positions:
        cost = f"${p['cost_basis_cents'] / 100:.2f}"
        pnl = f"${p['realized_pnl_cents'] / 100:.2f}"
        table.add_row(p["ticker"], str(p["signed_qty"]), cost, pnl, p["status"])

    rprint(table)


# ---------------------------------------------------------------------------
# session-status
# ---------------------------------------------------------------------------

@operator_app.command("session-status")
def session_status_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    as_json: bool = typer.Option(False, "--json", help="JSON output only."),
) -> None:
    """Show session status: events, kill-switch state, alerts, unknown orders."""
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    pending = PendingOrderRegistry(conn)

    events = store.load_session(session_id)

    # Derive kill switch state by replaying operator commands
    current_ks = "none"
    operator_commands = []
    session_started = False
    session_stopped = False
    alerts = []

    for e in events:
        if e.event_type == EventType.SESSION_STARTED:
            session_started = True
        elif e.event_type == EventType.SESSION_STOPPED:
            session_stopped = True
        elif e.event_type == EventType.OPERATOR_COMMAND:
            payload = e.payload
            cmd_type = getattr(payload, "command_type", "")
            params_str = getattr(payload, "parameters", None)
            params = json.loads(params_str) if params_str else {}
            operator_commands.append({
                "command_id": getattr(payload, "command_id", ""),
                "command_type": cmd_type,
                "parameters": params,
                "timestamp": e.occurred_at_utc,
            })
            if cmd_type == "kill_switch":
                current_ks = params.get("level", current_ks)

    # Count events by type
    event_counts: dict[str, int] = {}
    for e in events:
        key = e.event_type.value
        event_counts[key] = event_counts.get(key, 0) + 1

    # Get unknown orders
    unknown_orders = pending.get_unknown_orders()
    active_orders = pending.get_active_orders()

    # Options events summary
    options_events = {
        "assignments": event_counts.get("assignment_received", 0),
        "exercises": event_counts.get("exercise_processed", 0),
        "expirations": event_counts.get("expiration_processed", 0),
        "reconciliations": event_counts.get("reconciliation_completed", 0),
    }

    status = {
        "session_id": session_id,
        "total_events": len(events),
        "session_started": session_started,
        "session_stopped": session_stopped,
        "kill_switch_level": current_ks,
        "event_counts": event_counts,
        "options_lifecycle_events": options_events,
        "operator_commands": operator_commands,
        "active_orders": len(active_orders),
        "unknown_orders": len(unknown_orders),
    }

    conn.close()

    if as_json:
        rprint(json.dumps(status, indent=2))
        return

    rprint(f"\n[bold]Session Status[/bold] — [cyan]{session_id}[/cyan]")
    rprint(f"  Events: {len(events)}")

    # Session lifecycle
    if session_stopped:
        rprint("  Lifecycle: [dim]STOPPED[/dim]")
    elif session_started:
        rprint("  Lifecycle: [green]STARTED[/green]")
    else:
        rprint("  Lifecycle: [yellow]NO SESSION_STARTED EVENT[/yellow]")

    # Kill switch
    ks_color = {"none": "green", "close_only": "yellow", "full_halt": "red"}.get(current_ks, "white")
    rprint(f"  Kill switch: [{ks_color}]{current_ks.upper()}[/{ks_color}]")

    # Orders
    rprint(f"  Active orders: {len(active_orders)}")
    if unknown_orders:
        rprint(f"  [red]Unknown orders: {len(unknown_orders)}[/red]")

    # Options lifecycle
    if any(v > 0 for v in options_events.values()):
        rprint("\n  [bold]Options Lifecycle Events:[/bold]")
        for k, v in options_events.items():
            if v > 0:
                rprint(f"    {k}: {v}")

    # Event type breakdown
    rprint("\n  [bold]Event Breakdown:[/bold]")
    for etype, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        rprint(f"    {etype}: {count}")

    # Operator commands
    if operator_commands:
        rprint(f"\n  [bold]Operator Commands ({len(operator_commands)}):[/bold]")
        for cmd in operator_commands[-5:]:  # last 5
            rprint(f"    {cmd['timestamp']}: {cmd['command_type']} → {cmd['parameters']}")


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------

@operator_app.command("reconcile")
def reconcile_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    venue: str = typer.Option("alpaca", "--venue"),
    as_json: bool = typer.Option(False, "--json", help="JSON output only."),
) -> None:
    """Trigger on-demand reconciliation and display results.

    Persists OperatorCommandEvent before executing.
    Note: This is a dry-read reconciliation — it shows position state
    from the database. Full venue-aware reconciliation requires a live
    session with venue connectivity.
    """
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    pos_store = PositionStore(conn)

    # Persist operator command BEFORE action
    command_id = str(uuid.uuid4())
    cmd_event = CanonicalEvent(
        event_type=EventType.OPERATOR_COMMAND,
        occurred_at_utc=_now_utc_iso(),
        payload=OperatorCommandPayload(
            command_id=command_id,
            command_type="reconcile",
            parameters=json.dumps({"venue": venue, "mode": "on_demand"}),
            operator_id="cli",
        ),
    )
    store.append(session_id, cmd_event)

    # Read positions
    equity_pos = pos_store.get_equity_positions(session_id, venue)
    options_pos = pos_store.get_options_positions(session_id, venue)

    # Read pending orders
    pending = PendingOrderRegistry(conn)
    active = pending.get_active_orders()
    unknown = pending.get_unknown_orders()

    result = {
        "command_id": command_id,
        "session_id": session_id,
        "venue": venue,
        "equity_positions": equity_pos,
        "options_positions": options_pos,
        "active_orders": len(active),
        "unknown_orders": len(unknown),
        "unknown_order_ids": [o["client_order_id"] for o in unknown],
    }

    conn.close()

    if as_json:
        rprint(json.dumps(result, indent=2))
        return

    rprint(f"\n[bold]Reconciliation[/bold] — [cyan]{session_id[:12]}...[/cyan] @ {venue}")
    rprint(f"  Command: {command_id[:8]}...")
    rprint(f"  Equity positions: {len(equity_pos)}")
    rprint(f"  Options positions: {len(options_pos)}")
    rprint(f"  Active orders: {len(active)}")
    if unknown:
        rprint(f"  [red]Unknown orders: {len(unknown)}[/red]")
        for o in unknown[:5]:
            rprint(f"    → {o['client_order_id']}")
    else:
        rprint("  Unknown orders: 0 [green]✓[/green]")


# ---------------------------------------------------------------------------
# alerts
# ---------------------------------------------------------------------------

@operator_app.command("alerts")
def alerts_cmd(
    db_path: str = typer.Option(..., "--db-path"),
    session_id: str = typer.Option(..., "--session-id"),
    as_json: bool = typer.Option(False, "--json", help="JSON output only."),
) -> None:
    """Show operator alerts from event log for a session."""
    conn = open_sqlite(db_path)
    store = SqliteEventStore(conn)
    events = store.load_session(session_id)
    conn.close()

    # Collect alerts from event payloads
    alerts = []
    for e in events:
        # Assignment events generate implicit alerts
        if e.event_type == EventType.ASSIGNMENT_RECEIVED:
            payload = e.payload
            alerts.append({
                "severity": "critical",
                "category": "assignment",
                "message": f"Assignment received: {getattr(payload, 'instrument_occ_symbol', 'unknown')}",
                "timestamp": e.occurred_at_utc,
            })
        # Reconciliation breaks
        elif e.event_type == EventType.RECONCILIATION_COMPLETED:
            payload = e.payload
            blocking = getattr(payload, "blocking_break_count", 0)
            if blocking > 0:
                alerts.append({
                    "severity": "critical",
                    "category": "reconciliation_break",
                    "message": f"Reconciliation has {blocking} blocking break(s)",
                    "timestamp": e.occurred_at_utc,
                })
        # Operator kill-switch activations
        elif e.event_type == EventType.OPERATOR_COMMAND:
            payload = e.payload
            cmd_type = getattr(payload, "command_type", "")
            if cmd_type == "kill_switch":
                params = json.loads(getattr(payload, "parameters", "{}"))
                lvl = params.get("level", "?")
                if lvl in ("close_only", "full_halt"):
                    alerts.append({
                        "severity": "warning" if lvl == "close_only" else "critical",
                        "category": "kill_switch",
                        "message": f"Kill switch activated: {lvl}",
                        "timestamp": e.occurred_at_utc,
                    })

    if as_json:
        rprint(json.dumps(alerts, indent=2))
        return

    if not alerts:
        rprint(f"No alerts for session [cyan]{session_id}[/cyan]")
        return

    rprint(f"\n[bold]Alerts[/bold] — {session_id[:12]}... ({len(alerts)} total)")
    for a in alerts:
        sev = a["severity"]
        color = {"info": "blue", "warning": "yellow", "critical": "red"}.get(sev, "white")
        rprint(f"  [{color}]{sev.upper()}[/{color}] [{a['category']}] {a['message']} @ {a['timestamp']}")
