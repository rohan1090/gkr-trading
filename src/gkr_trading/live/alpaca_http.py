from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlencode

from gkr_trading.live.alpaca_config import AlpacaPaperConfig


class AlpacaHttpError(Exception):
    """Non-2xx Alpaca API response."""

    def __init__(self, status_code: int, body_text: str, parsed: dict[str, Any] | None) -> None:
        self.status_code = status_code
        self.body_text = body_text
        self.parsed = parsed
        super().__init__(f"Alpaca HTTP {status_code}: {body_text[:500]}")


class AlpacaSubmitUncertaintyError(Exception):
    """
    Submit outcome unknown (timeout, connection failure, ambiguous 5xx).
    Do not resubmit blindly: reconcile via GET /v2/orders filtered by client_order_id.
    """

    def __init__(self, message: str, *, client_order_id: str | None = None) -> None:
        self.client_order_id = client_order_id
        super().__init__(message)


class AlpacaSubmitUnresolvedError(Exception):
    """
    Post-uncertainty paginated broker search found no order for ``client_order_id``.
    Operator must reconcile manually; do not resubmit blindly.
    """

    def __init__(self, message: str, *, client_order_id: str) -> None:
        self.client_order_id = client_order_id
        super().__init__(message)


class AlpacaHttpClient(Protocol):
    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any: ...


@dataclass
class UrllibAlpacaHttpClient:
    """Thin JSON HTTP client for Alpaca (stdlib only)."""

    config: AlpacaPaperConfig
    timeout_sec: float = 30.0

    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.config.base_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        data: bytes | None = None
        headers = {
            "APCA-API-KEY-ID": self.config.api_key,
            "APCA-API-SECRET-KEY": self.config.secret_key,
        }
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        ctx = ssl.create_default_context()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec, context=ctx) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            parsed: dict[str, Any] | None = None
            try:
                parsed = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                parsed = None
            raise AlpacaHttpError(e.code, raw, parsed) from e
        except TimeoutError as e:
            raise AlpacaSubmitUncertaintyError(
                "Alpaca submit/request timed out",
                client_order_id=None,
            ) from e
        except OSError as e:
            raise AlpacaSubmitUncertaintyError(
                f"Alpaca network error: {e}",
                client_order_id=None,
            ) from e
