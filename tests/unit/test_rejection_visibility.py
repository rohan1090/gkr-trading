from __future__ import annotations

import json
from typing import Any

from typer.testing import CliRunner

from gkr_trading.cli import seed
from gkr_trading.cli.main import app


class RejectingAlpacaHttp:
    """POST /v2/orders returns immediate rejected with reason fields."""

    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        if method == "POST" and path == "/v2/orders":
            return {
                "id": "alp-rej-1",
                "status": "rejected",
                "reject_reason": "insufficient buying power",
                "status_description": "rejected: insufficient buying power",
            }
        if method == "GET" and path == "/v2/orders":
            return []
        if method == "GET" and "/account/activities" in path:
            return []
        return []


def _init_seed_db(tmp_path) -> str:
    db = str(tmp_path / "rej.db")
    conn = seed.initialize_database(db)
    seed.seed_instruments(conn)
    seed.seed_equity_bars(conn)
    conn.close()
    return db


def test_paper_report_surfaces_alpaca_reject_reason(tmp_path, monkeypatch) -> None:
    db = _init_seed_db(tmp_path)
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")

    import gkr_trading.live.alpaca_paper_adapter as apa

    monkeypatch.setattr(
        apa,
        "UrllibAlpacaHttpClient",
        lambda cfg, timeout_sec=30.0: RejectingAlpacaHttp(),
    )

    runner = CliRunner()
    r = runner.invoke(
        app,
        [
            "paper",
            "--db-path",
            db,
            "--json",
            "--session-id",
            "00000000-0000-4000-8000-00000000rj01",
        ],
    )
    assert r.exit_code == 0, r.stdout
    out = json.loads(r.stdout)
    assert out["adapter_mode"] == "alpaca"
    assert out["order_rejects"] >= 1
    assert out["broker_rejects_preview"], "expected rejected order preview"
    first = out["broker_rejects_preview"][0]
    assert first["reason_code"] in ("ALPACA_REJECTED", "ALPACA_HTTP_ERROR")
    assert "buying power" in (first["reason_detail"] or "").lower()


def test_session_inspect_surfaces_reject_reason(tmp_path, monkeypatch) -> None:
    db = _init_seed_db(tmp_path)
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")

    import gkr_trading.live.alpaca_paper_adapter as apa

    monkeypatch.setattr(
        apa,
        "UrllibAlpacaHttpClient",
        lambda cfg, timeout_sec=30.0: RejectingAlpacaHttp(),
    )

    runner = CliRunner()
    sid = "00000000-0000-4000-8000-00000000rj02"
    r = runner.invoke(app, ["paper", "--db-path", db, "--json", "--session-id", sid])
    assert r.exit_code == 0, r.stdout

    ins = runner.invoke(app, ["session-inspect", "--db-path", db, "--session-id", sid])
    assert ins.exit_code == 0, ins.stdout
    out = json.loads(ins.stdout)
    assert "order_rejects_preview" in out
    assert out["order_rejects_preview"]
    assert "buying power" in (out["order_rejects_preview"][0]["reason_detail"] or "").lower()

