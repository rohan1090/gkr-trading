"""Background worker that polls Alpaca /v2/positions and /v2/account every 10s."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional, List

logger = logging.getLogger(__name__)

ALPACA_POSITIONS_POLL_INTERVAL = 10.0


@dataclass
class LivePosition:
    ticker: str
    qty: float          # positive = long, negative = short
    side: str           # "long" or "short"
    avg_entry_cents: int
    last_cents: int
    unrealized_pnl_cents: int
    unrealized_pnl_pct: float
    market_value_cents: int
    cost_basis_cents: int
    asset_class: str    # "us_equity" or "us_option"


@dataclass
class LiveAccountSummary:
    cash_cents: int
    portfolio_value_cents: int
    buying_power_cents: int
    equity_cents: int
    unrealized_pnl_cents: int


class AlpacaPositionsWorker:
    """Polls Alpaca for live positions and account summary.

    Use a SEPARATE http client from the market data worker.
    This client points to paper-api.alpaca.markets (order management API).
    """

    def __init__(self) -> None:
        self._http = None
        self._available = False
        self._last_positions: List[LivePosition] = []
        self._last_account: Optional[LiveAccountSummary] = None
        self._init()

    def _init(self) -> None:
        try:
            from gkr_trading.live.alpaca_config import AlpacaPaperConfig
            from gkr_trading.live.alpaca_http import UrllibAlpacaHttpClient
            cfg = AlpacaPaperConfig.from_env()
            self._http = UrllibAlpacaHttpClient(config=cfg)
            self._available = True
        except Exception as exc:
            logger.warning(f"AlpacaPositionsWorker unavailable: {exc}")
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    def poll_once(self) -> tuple[List[LivePosition], Optional[LiveAccountSummary]]:
        """Fetch current positions and account state from Alpaca.
        Returns (positions, account_summary). Both can be empty/None on error.
        """
        if not self._available or not self._http:
            return [], None

        positions: List[LivePosition] = []
        account: Optional[LiveAccountSummary] = None

        # Fetch positions
        try:
            raw = self._http.request_json("GET", "/v2/positions")
            if isinstance(raw, list):
                for item in raw:
                    try:
                        pos = self._parse_position(item)
                        if pos:
                            positions.append(pos)
                    except Exception as exc:
                        logger.warning(f"Failed to parse position: {exc}")
        except Exception as exc:
            logger.error(f"Positions poll failed: {exc}")

        # Fetch account
        try:
            raw_acct = self._http.request_json("GET", "/v2/account")
            if isinstance(raw_acct, dict):
                account = LiveAccountSummary(
                    cash_cents=int(float(raw_acct.get("cash", 0)) * 100),
                    portfolio_value_cents=int(float(raw_acct.get("portfolio_value", 0)) * 100),
                    buying_power_cents=int(float(raw_acct.get("buying_power", 0)) * 100),
                    equity_cents=int(float(raw_acct.get("equity", 0)) * 100),
                    unrealized_pnl_cents=int(float(raw_acct.get("unrealized_pl", 0)) * 100),
                )
        except Exception as exc:
            logger.error(f"Account poll failed: {exc}")

        self._last_positions = positions
        self._last_account = account
        return positions, account

    def _parse_position(self, item: dict) -> Optional[LivePosition]:
        qty = float(item.get("qty", 0))
        side = item.get("side", "long")
        avg_entry = float(item.get("avg_entry_price", 0))
        current_price = float(item.get("current_price", 0))
        unrealized_pnl = float(item.get("unrealized_pl", 0))
        unrealized_pnl_pct = float(item.get("unrealized_plpc", 0)) * 100
        market_value = float(item.get("market_value", 0))
        cost_basis = float(item.get("cost_basis", 0))
        asset_class = item.get("asset_class", "us_equity")
        symbol = item.get("symbol", "")

        return LivePosition(
            ticker=symbol,
            qty=qty,
            side=side,
            avg_entry_cents=int(avg_entry * 100),
            last_cents=int(current_price * 100),
            unrealized_pnl_cents=int(unrealized_pnl * 100),
            unrealized_pnl_pct=unrealized_pnl_pct,
            market_value_cents=int(market_value * 100),
            cost_basis_cents=int(cost_basis * 100),
            asset_class=asset_class,
        )
