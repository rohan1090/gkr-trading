"""
Paginate Alpaca list endpoints until empty, short page, or max_pages.

Uses ``until`` = id of the *oldest* row in the previous page (last element when
direction=desc) to walk to older items. See Alpaca Trading API docs for
``GET /v2/orders`` and ``GET /v2/account/activities``.

If Alpaca changes pagination semantics, adjust here only (adapter-local).
"""

from __future__ import annotations

from typing import Any, Protocol

from gkr_trading.live.alpaca_http import AlpacaHttpClient


class AlpacaPaginatingClient(Protocol):
    def request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any: ...


def iter_order_pages_desc(
    http: AlpacaPaginatingClient,
    *,
    status: str = "all",
    page_limit: int = 100,
    max_pages: int = 50,
) -> tuple[list[list[dict[str, Any]]], int]:
    """
    Fetch successive pages of orders (newest-first per page).

    Returns (pages, page_count). Stops when a page is empty, shorter than
    ``page_limit``, or ``max_pages`` is reached.
    """
    pages: list[list[dict[str, Any]]] = []
    until: str | None = None
    for _ in range(max_pages):
        q: dict[str, str] = {
            "status": status,
            "limit": str(page_limit),
            "direction": "desc",
        }
        if until is not None:
            q["until"] = until
        raw = http.request_json("GET", "/v2/orders", query=q)
        if not isinstance(raw, list):
            break
        if not raw:
            break
        pages.append(raw)
        if len(raw) < page_limit:
            break
        last = raw[-1]
        oid = last.get("id")
        if not oid:
            break
        until = str(oid)
    return pages, len(pages)


def iter_fill_activity_pages_desc(
    http: AlpacaPaginatingClient,
    *,
    page_size: int = 100,
    max_pages: int = 50,
) -> tuple[list[list[dict[str, Any]]], int]:
    """Same pattern for FILL account activities."""
    pages: list[list[dict[str, Any]]] = []
    until: str | None = None
    for _ in range(max_pages):
        q: dict[str, str] = {
            "activity_types": "FILL",
            "page_size": str(page_size),
            "direction": "desc",
        }
        if until is not None:
            q["until"] = until
        raw = http.request_json("GET", "/v2/account/activities", query=q)
        if not isinstance(raw, list):
            break
        if not raw:
            break
        pages.append(raw)
        if len(raw) < page_size:
            break
        last = raw[-1]
        aid = last.get("id")
        if not aid:
            break
        until = str(aid)
    return pages, len(pages)
