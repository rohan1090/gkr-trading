"""WebSocket manager — real-time Alpaca trade_updates streaming.

Lifecycle:
  connect → subscribe → heartbeat loop → reconnect on failure
  - Suspends order submission while disconnected
  - Triggers on-demand reconciliation after reconnect before resuming

Uses Alpaca's streaming v2 trade_updates channel for fill events.
"""
from __future__ import annotations

import json
import logging
import ssl
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Alpaca paper WebSocket URL
ALPACA_PAPER_WS_URL = "wss://paper-api.alpaca.markets/stream"
ALPACA_PAPER_WS_V2_URL = "wss://stream.data.alpaca.markets/v2/test"
ALPACA_TRADE_UPDATES_URL = "wss://paper-api.alpaca.markets/stream"


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


@dataclass
class WebSocketStats:
    """Observable stats for monitoring."""
    connect_count: int = 0
    disconnect_count: int = 0
    reconnect_count: int = 0
    messages_received: int = 0
    last_heartbeat_ns: int = 0
    last_message_ns: int = 0
    consecutive_failures: int = 0


class AlpacaWebSocketManager:
    """Manages Alpaca WebSocket connection for trade_updates streaming.

    Thread-safe. Runs connection loop in a background thread.
    Callbacks are invoked from the receiver thread — callers must handle
    their own thread safety if updating shared state.
    """

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        *,
        ws_url: str = ALPACA_TRADE_UPDATES_URL,
        on_trade_update: Optional[Callable[[dict], None]] = None,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[str], None]] = None,
        on_reconnect: Optional[Callable[[], None]] = None,
        max_reconnect_attempts: int = 50,
        initial_backoff_sec: float = 1.0,
        max_backoff_sec: float = 60.0,
        heartbeat_interval_sec: float = 30.0,
        heartbeat_timeout_sec: float = 90.0,
    ) -> None:
        self._api_key = api_key
        self._secret_key = secret_key
        self._ws_url = ws_url
        self._on_trade_update = on_trade_update
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._on_reconnect = on_reconnect
        self._max_reconnect = max_reconnect_attempts
        self._initial_backoff = initial_backoff_sec
        self._max_backoff = max_backoff_sec
        self._heartbeat_interval = heartbeat_interval_sec
        self._heartbeat_timeout = heartbeat_timeout_sec

        self._state = ConnectionState.DISCONNECTED
        self._state_lock = threading.Lock()
        self._ws: Any = None  # websocket.WebSocket instance
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._stats = WebSocketStats()

    @property
    def state(self) -> ConnectionState:
        with self._state_lock:
            return self._state

    @property
    def stats(self) -> WebSocketStats:
        return self._stats

    @property
    def is_connected(self) -> bool:
        return self.state == ConnectionState.CONNECTED

    def start(self) -> None:
        """Start the WebSocket connection in a background thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._connection_loop,
            name="alpaca-ws-manager",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Gracefully stop the WebSocket connection."""
        self._stop_event.set()
        with self._state_lock:
            self._state = ConnectionState.CLOSED
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5.0)

    def _connection_loop(self) -> None:
        """Main loop: connect → receive → reconnect on failure."""
        backoff = self._initial_backoff
        while not self._stop_event.is_set():
            try:
                self._connect_and_subscribe()
                backoff = self._initial_backoff  # reset on successful connect
                self._stats.consecutive_failures = 0
                self._receive_loop()
            except Exception as exc:
                self._stats.disconnect_count += 1
                self._stats.consecutive_failures += 1
                logger.warning(f"WebSocket disconnected: {exc}")

                with self._state_lock:
                    if self._state == ConnectionState.CLOSED:
                        break
                    self._state = ConnectionState.RECONNECTING

                if self._on_disconnect:
                    try:
                        self._on_disconnect(str(exc))
                    except Exception:
                        pass

                if self._stats.consecutive_failures > self._max_reconnect:
                    logger.error("Max reconnect attempts exceeded")
                    break

                # Exponential backoff
                logger.info(f"Reconnecting in {backoff:.1f}s (attempt {self._stats.consecutive_failures})")
                self._stop_event.wait(timeout=backoff)
                backoff = min(backoff * 2, self._max_backoff)
                self._stats.reconnect_count += 1

    def _connect_and_subscribe(self) -> None:
        """Establish WebSocket connection, authenticate, subscribe to trade_updates."""
        try:
            import websocket as ws_lib
        except ImportError:
            # Fall back to using websocket-client if available,
            # otherwise this is a hard requirement
            raise ImportError(
                "websocket-client package required: pip install websocket-client"
            )

        with self._state_lock:
            self._state = ConnectionState.CONNECTING

        self._ws = ws_lib.create_connection(
            self._ws_url,
            timeout=self._heartbeat_timeout,
            sslopt={"cert_reqs": ssl.CERT_REQUIRED},
        )
        self._stats.connect_count += 1

        with self._state_lock:
            self._state = ConnectionState.AUTHENTICATING

        # Authenticate
        auth_msg = {
            "action": "auth",
            "key": self._api_key,
            "secret": self._secret_key,
        }
        self._ws.send(json.dumps(auth_msg))

        # Wait for auth response
        auth_resp_raw = self._ws.recv()
        auth_resp = json.loads(auth_resp_raw)
        logger.debug(f"Auth response: {auth_resp}")

        # Alpaca streaming API returns array of messages
        # Check for authorized message
        if isinstance(auth_resp, list):
            for msg in auth_resp:
                if msg.get("T") == "error":
                    raise ConnectionError(f"Auth failed: {msg}")
        elif isinstance(auth_resp, dict):
            if auth_resp.get("data", {}).get("status") == "authorized":
                pass
            elif auth_resp.get("stream") == "authorization" and auth_resp.get("data", {}).get("status") == "authorized":
                pass

        # Subscribe to trade_updates
        sub_msg = {
            "action": "listen",
            "data": {
                "streams": ["trade_updates"],
            },
        }
        self._ws.send(json.dumps(sub_msg))

        with self._state_lock:
            self._state = ConnectionState.CONNECTED

        self._stats.last_heartbeat_ns = time.time_ns()
        logger.info("WebSocket connected and subscribed to trade_updates")

        if self._on_connect:
            try:
                self._on_connect()
            except Exception:
                pass

        # Fire reconnect callback if this isn't the first connection
        if self._stats.connect_count > 1 and self._on_reconnect:
            try:
                self._on_reconnect()
            except Exception:
                pass

    def _receive_loop(self) -> None:
        """Receive and dispatch messages until disconnect or stop."""
        while not self._stop_event.is_set():
            try:
                raw = self._ws.recv()
                if not raw:
                    continue

                self._stats.messages_received += 1
                self._stats.last_message_ns = time.time_ns()
                self._stats.last_heartbeat_ns = time.time_ns()

                data = json.loads(raw)
                self._dispatch_message(data)

            except Exception as exc:
                if self._stop_event.is_set():
                    return
                raise  # will trigger reconnect in _connection_loop

    def _dispatch_message(self, data: Any) -> None:
        """Route incoming message to appropriate handler."""
        # Alpaca streaming sends different formats
        if isinstance(data, list):
            for msg in data:
                self._dispatch_single(msg)
        elif isinstance(data, dict):
            self._dispatch_single(data)

    def _dispatch_single(self, msg: dict) -> None:
        """Handle a single message."""
        # Alpaca trade_updates format
        stream = msg.get("stream", "")
        if stream == "trade_updates":
            trade_data = msg.get("data", msg)
            if self._on_trade_update:
                try:
                    self._on_trade_update(trade_data)
                except Exception as exc:
                    logger.error(f"trade_update callback error: {exc}")
            return

        # v2 streaming format
        msg_type = msg.get("T", "")
        if msg_type in ("t", "q", "b"):
            # market data (trade, quote, bar) — ignore for now
            return

        # Connection/subscription confirmations
        if msg_type in ("success", "subscription"):
            logger.debug(f"Streaming: {msg}")
            return

    def send_heartbeat(self) -> None:
        """Send a ping/heartbeat to keep connection alive."""
        if self._ws and self.is_connected:
            try:
                self._ws.ping()
                self._stats.last_heartbeat_ns = time.time_ns()
            except Exception:
                pass
