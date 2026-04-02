from __future__ import annotations

import json
import os
from typing import Any

from typer.testing import CliRunner

from gkr_trading.cli import seed
from gkr_trading.cli.main import app


class DummyAlpacaHttp:
    """Deterministic HTTP stub for AlpacaPaperAdapter (no network)."""

    def __init__(self) -> None:
        self._last_client_order_id: str | None = None
        self._alpaca_order_id = "alp-1"

    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        if method == "POST" and path == "/v2/orders":
            self._last_client_order_id = (json_body or {}).get("client_order_id")
            return {"id": self._alpaca_order_id, "status": "accepted"}
        if method == "GET" and path == "/v2/orders":
            cid = self._last_client_order_id or "unknown"
            return [{"id": self._alpaca_order_id, "client_order_id": cid, "status": "accepted"}]
        if method == "GET" and "/account/activities" in path:
            return []
        return []


def _init_seed_db(tmp_path) -> str:
    db = str(tmp_path / "sel.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    conn.close()
    return db


def test_auto_detect_env_missing_selects_mock(tmp_path, monkeypatch) -> None:
    db = _init_seed_db(tmp_path)
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    runner = CliRunner()
    r = runner.invoke(
        app,
        ["paper", "--db-path", db, "--json", "--session-id", "00000000-0000-4000-8000-00000000ad01"],
    )
    assert r.exit_code == 0, r.stdout
    out = json.loads(r.stdout)
    assert out["adapter_mode"] == "mock"


def test_auto_detect_env_present_selects_alpaca(tmp_path, monkeypatch) -> None:
    db = _init_seed_db(tmp_path)
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")

    # Ensure CLI-selected Alpaca adapter doesn't hit network.
    import gkr_trading.live.alpaca_paper_adapter as apa

    monkeypatch.setattr(apa, "UrllibAlpacaHttpClient", lambda cfg, timeout_sec=30.0: DummyAlpacaHttp())

    runner = CliRunner()
    r = runner.invoke(
        app,
        ["paper", "--db-path", db, "--json", "--session-id", "00000000-0000-4000-8000-00000000ad02"],
    )
    assert r.exit_code == 0, r.stdout
    out = json.loads(r.stdout)
    assert out["adapter_mode"] == "alpaca"


def test_cli_override_adapter_dry_run(tmp_path, monkeypatch) -> None:
    db = _init_seed_db(tmp_path)
    # Even if creds exist, explicit override should win.
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    runner = CliRunner()
    r = runner.invoke(
        app,
        [
            "paper",
            "--db-path",
            db,
            "--json",
            "--adapter",
            "dry_run",
            "--session-id",
            "00000000-0000-4000-8000-00000000ad03",
        ],
    )
    assert r.exit_code == 0, r.stdout
    out = json.loads(r.stdout)
    assert out["adapter_mode"] == "dry_run"

