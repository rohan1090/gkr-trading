"""Charles Schwab API adapter — market data and real account positions.

Uses stdlib ``urllib`` only (no ``requests``).  All prices normalised to
cents (int).  Auth via OAuth2 Bearer token from environment variables.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

SCHWAB_BASE_URL = "https://api.schwabapi.com"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"


# ── Exceptions ─────────────────────────────────────────────────────────

class SchwabApiError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


# ── Config ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SchwabConfig:
    access_token: str
    refresh_token: str
    client_id: str
    client_secret: str
    account_hash: str

    @classmethod
    def from_env(cls) -> SchwabConfig:
        """Read Schwab credentials from environment.  Raises if missing."""
        required = {
            "SCHWAB_ACCESS_TOKEN": "access_token",
            "SCHWAB_REFRESH_TOKEN": "refresh_token",
            "SCHWAB_CLIENT_ID": "client_id",
            "SCHWAB_CLIENT_SECRET": "client_secret",
            "SCHWAB_ACCOUNT_HASH": "account_hash",
        }
        vals: dict[str, str] = {}
        missing: list[str] = []
        for env_key, field_name in required.items():
            v = os.environ.get(env_key, "").strip()
            if not v:
                missing.append(env_key)
            vals[field_name] = v

        if missing:
            raise EnvironmentError(
                f"Missing Schwab env vars: {', '.join(missing)}"
            )
        return cls(**vals)


# ── HTTP client ────────────────────────────────────────────────────────

class SchwabHttpClient:
    """Low-level Schwab HTTP client using stdlib ``urllib``."""

    def __init__(self, config: SchwabConfig, timeout_sec: float = 30.0) -> None:
        self._config = config
        self._timeout = timeout_sec
        # Mutable copy for token refresh
        self._access_token = config.access_token

    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: Optional[dict] = None,
        body: Optional[dict] = None,
    ) -> Any:
        """Execute an HTTP request.  Returns parsed JSON.  Retries once on 5xx."""
        url = SCHWAB_BASE_URL + path
        if query:
            url += "?" + urllib.parse.urlencode(query)

        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        for attempt in range(2):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                status = exc.code
                body_text = ""
                try:
                    body_text = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    pass

                if status >= 500 and attempt == 0:
                    logger.warning(f"Schwab {status} — retrying in 2s")
                    time.sleep(2.0)
                    continue
                raise SchwabApiError(status, body_text)
            except Exception as exc:
                if attempt == 0:
                    time.sleep(2.0)
                    continue
                raise

    def refresh_access_token(self) -> str:
        """Exchange refresh_token for a new access_token.  Returns new token."""
        data = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self._config.refresh_token,
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
        }).encode("utf-8")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        req = urllib.request.Request(SCHWAB_TOKEN_URL, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        new_token = result.get("access_token", "")
        if not new_token:
            raise SchwabApiError(0, "Token refresh returned no access_token")
        self._access_token = new_token
        logger.info("Schwab access token refreshed successfully")
        return new_token


def _dollars_to_cents(val: Any) -> int:
    if val is None:
        return 0
    return int(float(val) * 100)


# ── Market Data ────────────────────────────────────────────────────────

class SchwabMarketDataAdapter:
    """Schwab market data — quotes and price history."""

    def __init__(self, client: SchwabHttpClient) -> None:
        self._client = client
        self._available = True

    def is_available(self) -> bool:
        return self._available

    def get_quotes(self, symbols: list[str]) -> dict[str, dict]:
        """Fetch real-time quotes for *symbols*.  Returns normalised dicts."""
        sym_str = ",".join(symbols)
        raw = self._client.request_json(
            "GET", "/marketdata/v1/quotes", query={"symbols": sym_str}
        )
        if not isinstance(raw, dict):
            return {}

        result: dict[str, dict] = {}
        for sym, data in raw.items():
            q = data.get("quote", {})
            result[sym] = {
                "last_cents": _dollars_to_cents(q.get("lastPrice")),
                "bid_cents": _dollars_to_cents(q.get("bidPrice")),
                "ask_cents": _dollars_to_cents(q.get("askPrice")),
                "open_cents": _dollars_to_cents(q.get("openPrice")),
                "high_cents": _dollars_to_cents(q.get("highPrice")),
                "low_cents": _dollars_to_cents(q.get("lowPrice")),
                "close_cents": _dollars_to_cents(q.get("closePrice")),
                "volume": q.get("totalVolume", 0),
                "timestamp_utc": q.get("quoteTimeInLong", ""),
            }
        return result

    def get_price_history(
        self,
        symbol: str,
        period_type: str = "day",
        period: int = 1,
        frequency_type: str = "minute",
        frequency: int = 1,
        extended_hours: bool = True,
    ) -> list[dict]:
        """Fetch historical OHLCV bars for *symbol*."""
        raw = self._client.request_json(
            "GET", "/marketdata/v1/pricehistory",
            query={
                "symbol": symbol,
                "periodType": period_type,
                "period": str(period),
                "frequencyType": frequency_type,
                "frequency": str(frequency),
                "needExtendedHoursData": str(extended_hours).lower(),
            },
        )
        candles = raw.get("candles", [])
        return [
            {
                "datetime_ms": c.get("datetime", 0),
                "open": c.get("open", 0.0),
                "high": c.get("high", 0.0),
                "low": c.get("low", 0.0),
                "close": c.get("close", 0.0),
                "volume": c.get("volume", 0),
            }
            for c in candles
        ]


# ── Positions ──────────────────────────────────────────────────────────

class SchwabPositionsAdapter:
    """Schwab account positions and summary."""

    def __init__(self, client: SchwabHttpClient) -> None:
        self._client = client
        self._account_hash = client._config.account_hash

    def get_positions(self) -> list[dict]:
        """Fetch positions from Schwab.  Returns normalised dicts."""
        raw = self._client.request_json(
            "GET", f"/trader/v1/accounts/{self._account_hash}",
            query={"fields": "positions"},
        )
        acct = raw.get("securitiesAccount", {})
        raw_positions = acct.get("positions", [])

        positions: list[dict] = []
        for p in raw_positions:
            inst = p.get("instrument", {})
            long_qty = float(p.get("longQuantity", 0))
            short_qty = float(p.get("shortQuantity", 0))
            qty = long_qty - short_qty
            side = "long" if qty >= 0 else "short"

            positions.append({
                "ticker": inst.get("symbol", ""),
                "qty": abs(qty),
                "side": side,
                "avg_entry_cents": _dollars_to_cents(p.get("averagePrice")),
                "last_cents": _dollars_to_cents(p.get("marketValue", 0) / max(abs(qty), 1)),
                "unrealized_pnl_cents": _dollars_to_cents(
                    p.get("longOpenProfitLoss", 0) + p.get("shortOpenProfitLoss", 0)
                ),
                "unrealized_pnl_pct": 0.0,
                "market_value_cents": _dollars_to_cents(p.get("marketValue")),
                "cost_basis_cents": _dollars_to_cents(
                    float(p.get("averagePrice", 0)) * abs(qty)
                ),
                "asset_class": inst.get("assetType", "EQUITY").lower(),
                "source": "schwab_live",
            })
        return positions

    def get_account_summary(self) -> dict:
        """Fetch account balances from Schwab."""
        raw = self._client.request_json(
            "GET", f"/trader/v1/accounts/{self._account_hash}",
        )
        acct = raw.get("securitiesAccount", {})
        balances = acct.get("currentBalances", {})

        return {
            "cash_cents": _dollars_to_cents(balances.get("cashBalance")),
            "portfolio_value_cents": _dollars_to_cents(balances.get("liquidationValue")),
            "buying_power_cents": _dollars_to_cents(balances.get("buyingPower")),
            "equity_cents": _dollars_to_cents(balances.get("equity")),
            "unrealized_pnl_cents": 0,
            "source": "schwab_live",
        }


# ── Module-level factories ─────────────────────────────────────────────

def is_schwab_available() -> bool:
    """True if all required Schwab environment variables are present."""
    required = [
        "SCHWAB_ACCESS_TOKEN", "SCHWAB_REFRESH_TOKEN",
        "SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET", "SCHWAB_ACCOUNT_HASH",
    ]
    return all(os.environ.get(k, "").strip() for k in required)


def create_schwab_adapters() -> tuple[SchwabMarketDataAdapter, SchwabPositionsAdapter]:
    """Create both Schwab adapters from environment.  Raises on missing vars."""
    config = SchwabConfig.from_env()
    client = SchwabHttpClient(config)
    return SchwabMarketDataAdapter(client), SchwabPositionsAdapter(client)
