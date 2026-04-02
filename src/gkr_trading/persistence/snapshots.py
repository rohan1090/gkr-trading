from __future__ import annotations

import json
import sqlite3

from gkr_trading.core.portfolio.models import PortfolioState


def ensure_snapshots_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            session_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (session_id, seq)
        )
        """
    )
    conn.commit()


def save_snapshot(conn: sqlite3.Connection, session_id: str, seq: int, state: PortfolioState) -> None:
    ensure_snapshots_table(conn)
    payload = {
        "cash": str(state.cash),
        "positions": {k: str(v) for k, v in state.positions.items()},
        "avg_entry": {k: str(v) for k, v in state.avg_entry.items()},
        "realized_pnl": str(state.realized_pnl),
        "unrealized_pnl": str(state.unrealized_pnl),
        "mark_prices": {k: str(v) for k, v in state.mark_prices.items()},
    }
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_snapshots(session_id, seq, payload_json) VALUES (?,?,?)",
        (session_id, seq, json.dumps(payload)),
    )
    conn.commit()
