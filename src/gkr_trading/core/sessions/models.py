from __future__ import annotations

from pydantic import BaseModel

from gkr_trading.core.schemas.ids import SessionId


class SessionMeta(BaseModel):
    model_config = {"frozen": True}

    session_id: SessionId
    mode: str  # "backtest" | "paper" | "live"
