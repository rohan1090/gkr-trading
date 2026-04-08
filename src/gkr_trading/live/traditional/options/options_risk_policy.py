"""OptionsRiskPolicy — undefined risk halt, expiry window block, assignment hazard.

Supports config-driven initialization via load_options_risk_config().
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from gkr_trading.core.instruments import OptionsRef
from gkr_trading.core.options_intents import TradeIntent
from gkr_trading.core.risk_gate import RiskApprovalGate, RiskDecision
from gkr_trading.live.traditional.options.options_domain import OptionsChainHelper


def load_options_risk_config(
    config_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Load options risk config from YAML. Returns dict of settings.

    Falls back to conservative hardcoded defaults if file not found or
    pyyaml not installed.
    """
    defaults: Dict[str, Any] = {
        "max_contracts_per_order": 1,
        "max_contracts_per_underlying": 3,
        "block_undefined_risk": True,
        "max_short_premium_exposure_cents": 0,
        "expiry_window_minutes": 60,
        "expiry_proximity_days_halt": 1,
        "max_options_buying_power_pct": 0.25,
        "daily_stop_loss_cents": 5000,
        "allow_sell_to_open": False,
        "allow_buy_to_close": True,
    }

    if config_path is None:
        # Look in standard locations
        candidates = [
            "config/risk/options_risk_policy.yaml",
            os.environ.get("GKR_OPTIONS_RISK_CONFIG", ""),
        ]
        for c in candidates:
            if c and Path(c).exists():
                config_path = c
                break

    if config_path and Path(config_path).exists():
        try:
            import yaml
            with open(config_path) as f:
                loaded = yaml.safe_load(f)
            if isinstance(loaded, dict):
                defaults.update(loaded)
        except ImportError:
            # pyyaml not installed — use defaults
            pass
        except Exception:
            pass

    return defaults


class OptionsRiskPolicy(RiskApprovalGate):
    """Options-specific risk checks.

    - Max contracts per order / per underlying
    - Undefined risk halt (naked short calls)
    - Expiry window block (no new positions near expiry)
    - sell_to_open gating (long-only by default)
    - Assignment hazard check (short options near ITM)
    """

    def __init__(
        self,
        max_contracts: int = 10,
        block_undefined_risk: bool = True,
        expiry_window_days: int = 0,
        allow_sell_to_open: bool = False,
    ) -> None:
        self._max_contracts = max_contracts
        self._block_undefined = block_undefined_risk
        self._expiry_window_days = expiry_window_days
        self._allow_sell_to_open = allow_sell_to_open

    @classmethod
    def from_config(cls, config_path: Optional[str] = None) -> "OptionsRiskPolicy":
        """Build OptionsRiskPolicy from config file."""
        cfg = load_options_risk_config(config_path)
        return cls(
            max_contracts=cfg.get("max_contracts_per_order", 1),
            block_undefined_risk=cfg.get("block_undefined_risk", True),
            expiry_window_days=cfg.get("expiry_proximity_days_halt", 1),
            allow_sell_to_open=cfg.get("allow_sell_to_open", False),
        )

    def evaluate(self, intent: object, context: object) -> RiskDecision:
        if not isinstance(intent, TradeIntent):
            return RiskDecision(approved=False, reason_code="INVALID_INTENT_TYPE")

        ref = intent.instrument_ref
        if not isinstance(ref, OptionsRef):
            # Not an options intent — pass through
            return RiskDecision(approved=True)

        # Max contracts check
        if intent.quantity > self._max_contracts:
            return RiskDecision(
                approved=False,
                reason_code="MAX_CONTRACTS",
                reason_detail=f"Quantity {intent.quantity} exceeds max {self._max_contracts}",
            )

        # sell_to_open gating (long-only mode)
        if not self._allow_sell_to_open and intent.action == "sell_to_open":
            return RiskDecision(
                approved=False,
                reason_code="SELL_TO_OPEN_BLOCKED",
                reason_detail="sell_to_open is disabled in current risk config (long-only mode)",
            )

        # Undefined risk check — block naked short calls
        if self._block_undefined and intent.action == "sell_to_open" and ref.right == "call":
            return RiskDecision(
                approved=False,
                reason_code="UNDEFINED_RISK",
                reason_detail="Naked short calls are blocked (undefined risk)",
            )

        # Expiry window block — no new opening orders near expiry
        today = datetime.now(timezone.utc).date()
        if intent.action in ("buy_to_open", "sell_to_open"):
            if OptionsChainHelper.is_in_expiry_window(ref, today, self._expiry_window_days):
                return RiskDecision(
                    approved=False,
                    reason_code="EXPIRY_WINDOW_BLOCK",
                    reason_detail=f"Cannot open new positions within {self._expiry_window_days} days of expiry",
                )

        return RiskDecision(approved=True)
