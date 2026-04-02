"""
Thin Alpaca **paper** trading adapter: REST submit + poll only (no websockets, no live cash).

Maps Alpaca order/activity payloads into existing BrokerFact contracts consumed by broker_sync.

Hardening: paginated polls, persisted adapter state (SQLite), submit-uncertainty resolution
via client_order_id search (no blind resubmit).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from gkr_trading.live.alpaca_config import AlpacaPaperConfig
from gkr_trading.live.alpaca_http import (
    AlpacaHttpClient,
    AlpacaHttpError,
    AlpacaSubmitUncertaintyError,
    UrllibAlpacaHttpClient,
)
from gkr_trading.live.alpaca_pagination import iter_fill_activity_pages_desc, iter_order_pages_desc
from gkr_trading.live.alpaca_paper_mapping import (
    AlpacaMalformedPayloadError,
    fill_activity_to_broker_fill_fact,
    order_to_lifecycle_facts,
)
from gkr_trading.live.broker_adapter import (
    BrokerFact,
    BrokerFillFact,
    BrokerPollHints,
    BrokerPollResult,
    BrokerReconciliationCursor,
    BrokerSyncPhase,
    SubmissionResult,
    SubmitRequest,
)
from gkr_trading.live.submit_request_json import submit_request_from_jsonable, submit_request_to_jsonable


def _hints_ts(hints: BrokerPollHints) -> str:
    return hints.default_occurred_at_utc or hints.bar_ts_utc or "1970-01-01T00:00:00Z"


def _build_order_body(req: SubmitRequest) -> dict[str, Any]:
    sym = req.executable_broker_symbol
    if not sym or not sym.strip():
        raise ValueError(
            "AlpacaPaperAdapter requires SubmitRequest.executable_broker_symbol; "
            "use run_paper_session(symbol_resolver=...) with instrument master mapping."
        )
    qty = req.quantity
    if qty != qty.to_integral_value():
        raise ValueError(
            f"Alpaca paper equity orders require whole-share qty; got {qty!r}"
        )
    body: dict[str, Any] = {
        "symbol": sym.strip().upper(),
        "qty": str(int(qty)),
        "side": req.side.value,
        "type": req.order_type.value,
        "time_in_force": "day",
        "client_order_id": str(req.order_id),
    }
    if req.order_type.value == "limit":
        if req.limit_price is None:
            raise ValueError("limit order missing limit_price")
        body["limit_price"] = str(req.limit_price)
    return body


PERSISTED_VERSION = 2


@dataclass(frozen=True)
class SubmitUncertaintyResolution:
    """Outcome of ``resolve_submit_uncertainty`` (broker lookup by client_order_id)."""

    found: bool
    submission_result: SubmissionResult | None = None
    detail: str | None = None


class AlpacaPaperAdapter:
    """
    BrokerAdapter for Alpaca paper REST only.

    - Tracks client_order_id -> SubmitRequest for fill enrichment (symbol -> instrument_id).
    - Poll uses paginated GET /v2/orders and /v2/account/activities (bounded pages).
    - ``export_persisted_payload`` / ``import_persisted_payload`` for SQLite broker state.
    - Does not mutate PortfolioState.
    """

    def __init__(
        self,
        config: AlpacaPaperConfig,
        *,
        http: AlpacaHttpClient | None = None,
        timeout_sec: float = 30.0,
    ) -> None:
        self._cfg = config
        self._http = http or UrllibAlpacaHttpClient(config, timeout_sec=timeout_sec)
        self._by_client: dict[str, SubmitRequest] = {}
        self._alpaca_id_by_client: dict[str, str] = {}
        self._emitted_reject: set[str] = set()
        self._emitted_cancel: set[str] = set()
        self._emitted_fill_activity_ids: set[str] = set()
        self._last_order_pages_polled: int = 0
        self._last_activity_pages_polled: int = 0
        self._last_uncertainty_resolve_order_pages: int = 0

    def last_poll_page_counts(self) -> tuple[int, int]:
        """(order_pages, activity_pages) for the most recent ``poll_broker_facts`` call."""
        return self._last_order_pages_polled, self._last_activity_pages_polled

    def last_uncertainty_resolve_pages(self) -> int:
        return self._last_uncertainty_resolve_order_pages

    def export_persisted_payload(self) -> dict[str, Any]:
        tracked: list[dict[str, Any]] = []
        for cid, req in self._by_client.items():
            tracked.append(
                {
                    "client_order_id": cid,
                    "alpaca_order_id": self._alpaca_id_by_client.get(cid),
                    "req": submit_request_to_jsonable(req),
                }
            )
        return {
            "version": PERSISTED_VERSION,
            "emitted_fill_activity_ids": sorted(self._emitted_fill_activity_ids),
            "emitted_reject_clients": sorted(self._emitted_reject),
            "emitted_cancel_clients": sorted(self._emitted_cancel),
            "tracked": tracked,
        }

    def import_persisted_payload(self, payload: dict[str, Any]) -> None:
        if not payload:
            return
        self._emitted_fill_activity_ids = set(payload.get("emitted_fill_activity_ids") or [])
        self._emitted_reject = set(payload.get("emitted_reject_clients") or [])
        self._emitted_cancel = set(payload.get("emitted_cancel_clients") or [])
        self._by_client.clear()
        self._alpaca_id_by_client.clear()
        for row in payload.get("tracked") or []:
            cid = row.get("client_order_id")
            req_d = row.get("req")
            aid = row.get("alpaca_order_id")
            if not cid or not req_d:
                continue
            self._by_client[str(cid)] = submit_request_from_jsonable(req_d)
            if aid:
                self._alpaca_id_by_client[str(cid)] = str(aid)

    def merge_rehydrated_tracked(
        self,
        by_client: dict[str, SubmitRequest],
        alpaca_id_by_client: dict[str, str],
        *,
        union_fill_emitted: set[str],
    ) -> None:
        """
        Merge event-log rehydration (open orders). Does not remove existing tracked rows.
        ``union_fill_emitted`` seeds dedupe (e.g. broker_execution_id from canonical log).
        """
        self._emitted_fill_activity_ids |= union_fill_emitted
        for cid, req in by_client.items():
            if cid not in self._by_client:
                self._by_client[cid] = req
            aid = alpaca_id_by_client.get(cid)
            if aid and cid not in self._alpaca_id_by_client:
                self._alpaca_id_by_client[cid] = aid

    def resolve_submit_uncertainty(self, req: SubmitRequest) -> SubmitUncertaintyResolution:
        """
        After ``AlpacaSubmitUncertaintyError``, search broker orders by ``client_order_id``.

        Alpaca does not offer a single GET-by-client-order-id; we paginate ``/v2/orders``.
        If the order was created but not yet visible, this can return *not found* — safest
        V1 behavior is to treat that as unresolved and require operator follow-up (no resubmit).
        """
        cid = str(req.order_id)
        pages, npg = iter_order_pages_desc(self._http, status="all", max_pages=50)
        self._last_uncertainty_resolve_order_pages = npg
        ts = req.context_ts_utc or _hints_ts(
            BrokerPollHints(bar_ts_utc=None, default_occurred_at_utc=None)
        )
        for page in pages:
            for order in page:
                if order.get("client_order_id") != cid:
                    continue
                broker_id = order.get("id")
                st = (order.get("status") or "").lower()
                if st == "rejected":
                    msg = order.get("reject_reason") or order.get("status_description") or ""
                    return SubmitUncertaintyResolution(
                        found=True,
                        detail="broker_order_rejected",
                        submission_result=SubmissionResult(
                            occurred_at_utc=ts,
                            broker_order_id=str(broker_id) if broker_id else None,
                            rejected=True,
                            reject_reason_code="ALPACA_REJECTED",
                            reject_reason_detail=str(msg) or None,
                        ),
                    )
                self._by_client[cid] = req
                if broker_id:
                    self._alpaca_id_by_client[cid] = str(broker_id)
                return SubmitUncertaintyResolution(
                    found=True,
                    detail="broker_order_recovered",
                    submission_result=SubmissionResult(
                        occurred_at_utc=ts,
                        broker_order_id=str(broker_id) if broker_id else None,
                        rejected=False,
                    ),
                )
        return SubmitUncertaintyResolution(
            found=False,
            detail=(
                "Paginated /v2/orders search found no matching client_order_id "
                "(order may not exist, or visibility delayed beyond this pass)."
            ),
        )

    def submit(self, req: SubmitRequest) -> SubmissionResult:
        cid = str(req.order_id)
        ts = req.context_ts_utc or _hints_ts(
            BrokerPollHints(bar_ts_utc=None, default_occurred_at_utc=None)
        )
        body = _build_order_body(req)
        try:
            data = self._http.request_json("POST", "/v2/orders", json_body=body)
        except AlpacaSubmitUncertaintyError as e:
            raise AlpacaSubmitUncertaintyError(
                str(e), client_order_id=cid,
            ) from e
        except AlpacaHttpError as e:
            return self._submission_result_from_http_error(e, occurred_at_utc=ts, client_order_id=cid)

        if not isinstance(data, dict):
            raise AlpacaSubmitUncertaintyError(
                "Alpaca submit returned non-object body",
                client_order_id=cid,
            )

        broker_id = data.get("id")
        status = (data.get("status") or "").lower()

        if status == "rejected":
            msg = data.get("reject_reason") or data.get("status_description") or "rejected"
            return SubmissionResult(
                occurred_at_utc=ts,
                broker_order_id=str(broker_id) if broker_id else None,
                rejected=True,
                reject_reason_code="ALPACA_REJECTED",
                reject_reason_detail=str(msg),
            )

        self._by_client[cid] = req
        if broker_id:
            self._alpaca_id_by_client[cid] = str(broker_id)

        return SubmissionResult(
            occurred_at_utc=ts,
            broker_order_id=str(broker_id) if broker_id else None,
            rejected=False,
        )

    def _submission_result_from_http_error(
        self,
        e: AlpacaHttpError,
        *,
        occurred_at_utc: str,
        client_order_id: str,
    ) -> SubmissionResult:
        if e.status_code in (429,) or e.status_code >= 500:
            raise AlpacaSubmitUncertaintyError(
                f"Alpaca submit uncertain (HTTP {e.status_code})",
                client_order_id=client_order_id,
            ) from e
        code = "ALPACA_HTTP_ERROR"
        detail = e.body_text[:2000]
        if e.parsed:
            msg = e.parsed.get("message") or e.parsed.get("error")
            if msg:
                detail = str(msg)
            c = e.parsed.get("code")
            if c:
                code = str(c)[:80]
        return SubmissionResult(
            occurred_at_utc=occurred_at_utc,
            broker_order_id=None,
            rejected=True,
            reject_reason_code=code,
            reject_reason_detail=detail,
        )

    def poll_broker_facts(
        self,
        *,
        cursor: BrokerReconciliationCursor | None,
        hints: BrokerPollHints,
        phase: BrokerSyncPhase,
    ) -> BrokerPollResult:
        del phase
        ts = _hints_ts(hints)
        facts: list[BrokerFact] = []
        self._last_order_pages_polled = 0
        self._last_activity_pages_polled = 0
        facts.extend(self._poll_order_lifecycle(ts))
        facts.extend(self._poll_fill_activities(ts))
        next_tok = (cursor.token + "|") if cursor and cursor.token else ""
        next_tok += f"o{self._last_order_pages_polled}:a{self._last_activity_pages_polled}"
        return BrokerPollResult(tuple(facts), BrokerReconciliationCursor(token=next_tok))

    def _poll_order_lifecycle(self, fallback_ts: str) -> list[BrokerFact]:
        out: list[BrokerFact] = []
        if not self._by_client:
            return out
        pages, npc = iter_order_pages_desc(self._http, status="all", max_pages=50)
        self._last_order_pages_polled = npc
        for page in pages:
            for order in page:
                cid = order.get("client_order_id")
                if not cid or cid not in self._by_client:
                    continue
                submit = self._by_client[cid]
                st = (order.get("status") or "").lower()
                if st == "rejected" and cid not in self._emitted_reject:
                    for f in order_to_lifecycle_facts(order, submit=submit, fallback_ts=fallback_ts):
                        out.append(f)
                    self._emitted_reject.add(cid)
                elif st in ("canceled", "cancelled", "expired", "done_for_day") and cid not in self._emitted_cancel:
                    for f in order_to_lifecycle_facts(order, submit=submit, fallback_ts=fallback_ts):
                        out.append(f)
                    self._emitted_cancel.add(cid)
        return out

    def _poll_fill_activities(self, fallback_ts: str) -> list[BrokerFillFact]:
        out: list[BrokerFillFact] = []
        if not self._alpaca_id_by_client:
            return out
        tracked_alpaca_ids = set(self._alpaca_id_by_client.values())
        pages, npc = iter_fill_activity_pages_desc(self._http, max_pages=50)
        self._last_activity_pages_polled = npc
        for page in pages:
            for act in page:
                if (act.get("activity_type") or "").upper() != "FILL":
                    continue
                aid = act.get("id")
                if not aid or aid in self._emitted_fill_activity_ids:
                    continue
                oid = act.get("order_id")
                if not oid or str(oid) not in tracked_alpaca_ids:
                    continue
                cid = self._client_for_alpaca_order_id(str(oid))
                if cid is None:
                    continue
                submit = self._by_client[cid]
                fact = fill_activity_to_broker_fill_fact(
                    act, submit=submit, fallback_ts=fallback_ts
                )
                self._emitted_fill_activity_ids.add(str(aid))
                out.append(fact)
        return out

    def _client_for_alpaca_order_id(self, alpaca_order_id: str) -> str | None:
        for cid, aid in self._alpaca_id_by_client.items():
            if aid == alpaca_order_id:
                return cid
        return None
