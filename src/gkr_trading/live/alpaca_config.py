from __future__ import annotations

import os
from dataclasses import dataclass


class AlpacaConfigError(ValueError):
    """Missing or invalid Alpaca paper configuration."""


DEFAULT_PAPER_BASE_URL = "https://paper-api.alpaca.markets"


@dataclass(frozen=True)
class AlpacaPaperConfig:
    """Credentials and base URL for Alpaca paper trading API only."""

    api_key: str
    secret_key: str
    base_url: str = DEFAULT_PAPER_BASE_URL

    @staticmethod
    def from_env() -> AlpacaPaperConfig:
        key = os.environ.get("ALPACA_API_KEY", "").strip()
        secret = os.environ.get("ALPACA_SECRET_KEY", "").strip()
        if not key or not secret:
            raise AlpacaConfigError(
                "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set for AlpacaPaperAdapter"
            )
        base = os.environ.get("ALPACA_PAPER_BASE_URL", DEFAULT_PAPER_BASE_URL).strip()
        if not base:
            base = DEFAULT_PAPER_BASE_URL
        return AlpacaPaperConfig(api_key=key, secret_key=secret, base_url=base.rstrip("/"))
