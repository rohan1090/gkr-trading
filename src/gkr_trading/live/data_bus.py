"""In-process publish/subscribe event bus — central nervous system of the observation plane.

Decouples data producers (pollers) from consumers (TUI, execution engine).
One bad subscriber never blocks others — exceptions are caught and logged.
Zero external dependencies — stdlib only.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable

logger = logging.getLogger(__name__)

# ── Topic constants ────────────────────────────────────────────────────
TOPIC_MARKET_SNAPSHOT = "market.snapshot"
TOPIC_MARKET_STATUS = "market.status"
TOPIC_POSITIONS = "positions.update"
TOPIC_ACCOUNT = "account.update"
TOPIC_OHLCV_BAR = "market.ohlcv_bar"


class DataBus:
    """Thread-safe publish/subscribe event bus."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[dict], None]]] = {}
        self._lock = threading.Lock()

    def subscribe(self, topic: str, callback: Callable[[dict], None]) -> None:
        """Register *callback* for *topic*.  Thread-safe and idempotent."""
        with self._lock:
            subs = self._subscribers.setdefault(topic, [])
            if callback not in subs:
                subs.append(callback)

    def unsubscribe(self, topic: str, callback: Callable) -> None:
        """Remove *callback* from *topic*.  No-op if not registered."""
        with self._lock:
            if topic in self._subscribers:
                try:
                    self._subscribers[topic].remove(callback)
                except ValueError:
                    pass

    def publish(self, topic: str, payload: dict) -> None:
        """Synchronous publish — calls all subscribers in order.

        Each callback is wrapped in try/except so one failure never
        prevents other subscribers from receiving the event.
        """
        with self._lock:
            callbacks = list(self._subscribers.get(topic, []))
        for cb in callbacks:
            try:
                cb(payload)
            except Exception as exc:
                logger.error(
                    f"DataBus subscriber error on topic '{topic}': {exc}",
                    exc_info=True,
                )

    def publish_async(self, topic: str, payload: dict) -> None:
        """Asynchronous publish — each callback in its own daemon thread.

        Use for heavy consumers that should not block the poller.
        """
        with self._lock:
            callbacks = list(self._subscribers.get(topic, []))
        for cb in callbacks:
            t = threading.Thread(
                target=self._safe_call,
                args=(topic, cb, payload),
                daemon=True,
            )
            t.start()

    @staticmethod
    def _safe_call(topic: str, cb: Callable, payload: dict) -> None:
        try:
            cb(payload)
        except Exception as exc:
            logger.error(
                f"DataBus async subscriber error on topic '{topic}': {exc}",
                exc_info=True,
            )


# ── Module-level singleton ─────────────────────────────────────────────
_default_bus: DataBus | None = None
_bus_lock = threading.Lock()


def get_default_bus() -> DataBus:
    """Return the process-wide DataBus singleton (created on first call)."""
    global _default_bus
    with _bus_lock:
        if _default_bus is None:
            _default_bus = DataBus()
        return _default_bus
