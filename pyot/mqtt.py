from __future__ import annotations

import logging
import os
import socket
import ssl
import sys
import threading
import uuid
from typing import Any, Callable, Optional, Union

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from paho.mqtt.client import Client, MQTTMessage, topic_matches_sub

MessageHandler = Callable[[str, bytes], None]
ConnectHandler = Callable[[], None]
DisconnectHandler = Callable[[int], None]


class MQTTClient:
    """Wrapper around paho-mqtt client.

    Alows subscribing to multiple topic filters with optional per-filter handlers.
    """

    def __init__(
        self,
        host: str,
        port: int = 1883,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        tls_ca: Optional[str] = None,
        keepalive: int = 60,
        qos: int = 1,
        on_message: Optional[MessageHandler] = None,
        on_connect: Optional[ConnectHandler] = None,
        on_disconnect: Optional[DisconnectHandler] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Constructor.

        Args:
            host: MQTT broker hostname or IP address.
            port: MQTT broker port (default 1883, 8883 for TLS).
            username: Optional username for broker authentication.
            password: Optional password for broker authentication.
            tls_ca: Optional path to CA certificate file for TLS connection.
            keepalive: Keepalive interval in seconds (default 60).
            qos: Default QoS for subscriptions and publishes (0, 1, or 2; default 1).
            on_message: Optional default message handler (topic, payload).
            on_connect: Optional connect handler().
            on_disconnect: Optional disconnect handler(reason_code).
            logger: Optional logger. If None, uses a module-level logger with NullHandler.
        """

        # Store connection params
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.qos = qos

        # External logger or a no-op logger
        self.log = logger or logging.getLogger(__name__)
        if not logger:
            self.log.addHandler(logging.NullHandler())

        # Store callbacks (all optional)
        self._user_on_message = on_message
        self._user_on_connect = on_connect
        self._user_on_disconnect = on_disconnect

        # Initialize topic routing
        self._routes: dict[str, Optional[MessageHandler]] = {}
        self._route_qos: dict[str, int] = {}

        # Build client
        self._client = mqtt.Client(
            client_id=f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}",
            protocol=mqtt.MQTTv5,
        )
        if username:
            self._client.username_pw_set(username, password)
        if tls_ca:
            self._client.tls_set(
                ca_certs=tls_ca,
                certfile=None,
                keyfile=None,
                cert_reqs=ssl.CERT_REQUIRED,
            )
            self._client.tls_insecure_set(False)
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)

        # Bind internal handlers that delegate to user handlers
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # Initialize state
        self._loop_running = False
        self._lock = threading.RLock()

    # ---------- Public API ----------

    def subscribe(
        self,
        topic_filter: str,
        *,
        handler: Optional[MessageHandler] = None,
        qos: Optional[int] = None,
    ) -> None:
        """
        Subscribe to a topic filter (wildcards allowed) and optionally attach a handler.

        If handler is None, messages for this filter fall back to the default on_message
        handler. If qos is None, uses the client's default qos.

        Args:
            topic_filter: MQTT topic filter to subscribe to.
            handler: Optional message handler (topic, payload) for this filter.
            qos: Optional QoS for this subscription (0, 1, or 2).
        """
        with self._lock:

            # Store the handler and QoS for this filter
            self._routes[topic_filter] = handler
            self._route_qos[topic_filter] = self.qos if qos is None else qos

            # If already connected, subscribe immediately
            if self._loop_running:
                q = self._route_qos[topic_filter]
                self.log.info("MQTT subscribing to %s (qos=%d)", topic_filter, q)
                result, _ = self._client.subscribe(topic_filter, qos=q)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    self.log.error(
                        "Failed to subscribe to %s: code=%s", topic_filter, result
                    )

    def unsubscribe(self, topic_filter: str) -> None:
        """Unsubscribe and remove any handler for the given filter.

        Args:
            topic_filter: MQTT topic filter to unsubscribe from.
        """
        with self._lock:
            if topic_filter in self._routes:
                del self._routes[topic_filter]
                self._route_qos.pop(topic_filter, None)
                if self._loop_running:
                    self._client.unsubscribe(topic_filter)
                    self.log.info("MQTT unsubscribed from %s", topic_filter)

    def clear_subscriptions(self) -> None:
        """Remove all subscriptions and handlers."""
        with self._lock:
            if self._loop_running and self._routes:
                for f in list(self._routes.keys()):
                    self._client.unsubscribe(f)
            self._routes.clear()
            self._route_qos.clear()

    def start(self) -> None:
        """
        Connect and start the network loop in a background thread.
        """
        with self._lock:
            if self._loop_running:
                return
            self.log.info("MQTT connecting to %s:%s ...", self.host, self.port)
            try:
                self._client.connect(self.host, self.port, keepalive=self.keepalive)
            except Exception as e:
                self.log.exception("MQTT initial connect failed: %s", e)
            self._client.loop_start()
            self._loop_running = True

    def stop(self) -> None:
        """
        Stop the network loop and disconnect cleanly.
        """
        with self._lock:
            if not self._loop_running:
                return
            try:
                self._client.loop_stop()
                self._client.disconnect()
            finally:
                self._loop_running = False
                self.log.info("MQTT disconnected.")

    def publish(
        self,
        topic: str,
        payload: Union[str, bytes],
        qos: Optional[int] = None,
        retain: bool = False,
    ) -> mqtt.MQTTMessageInfo:
        """
        Publish payload to topic on server.

        Args:
            topic: Topic to publish to.
            payload: Message payload (str or bytes).
            qos: Optional QoS for this message (0, 1, or 2).
            retain: If True, set the retain flag on the message.

        Returns:
            MQTTMessageInfo object for the publish request.
        """
        q = self.qos if qos is None else qos
        self.log.debug("MQTT publish topic=%s qos=%s retain=%s", topic, q, retain)
        return self._client.publish(topic, payload=payload, qos=q, retain=retain)

    def _on_connect(
        self,
        client: Client,
        userdata: Any,
        flags: dict[str, Any],
        reason_code: int,
        *extra: Any,
    ) -> None:
        """Internal connect handler that subscribes to all registered filters.
        Delegates to user on_connect handler if set.

        Args:
            client: The client instance for this callback.
            userdata: The private user data as set in Client() or userdata_set().
            flags: Response flags sent by the broker.
            reason_code: The connection result.
            extra: Additional arguments.
        """
        if reason_code == 0:
            self.log.info("MQTT connected.")
            with self._lock:
                for filt, q in self._route_qos.items():
                    self.log.info("MQTT subscribing to %s (qos=%d)", filt, q)
                    result, _ = client.subscribe(filt, qos=q)
                    if result != mqtt.MQTT_ERR_SUCCESS:
                        self.log.error(
                            "Failed to subscribe to %s: code=%s", filt, result
                        )
        else:
            self.log.warning(
                "MQTT connect returned non-zero result: rc=%s", reason_code
            )
        if self._user_on_connect:
            try:
                self._user_on_connect()
            except Exception:
                self.log.exception("Error in user on_connect handler")

    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        reason_code: int,
        *extra: Any,
    ) -> None:
        """Internal disconnect handler that logs and delegates to user handler if set.

        Args:
            client: The client instance for this callback.
            userdata: The private user data as set in Client() or userdata_set().
            reason_code: The disconnection reason.
            extra: Additional arguments.
        """
        if reason_code != 0:
            self.log.warning(
                "MQTT unexpectedly disconnected (rc=%s). Will auto-reconnect.",
                reason_code,
            )
        else:
            self.log.info("MQTT cleanly disconnected.")
        if self._user_on_disconnect:
            try:
                self._user_on_disconnect(reason_code)
            except Exception:
                self.log.exception("Error in user on_disconnect handler")

    def _on_message(
        self,
        client: Client,
        userdata: Any,
        msg: MQTTMessage,
    ) -> None:
        """Internal message handler that routes to the most specific matching handler.

        If no specific handler matches, falls back to the default user on_message handler.

        Args:
            client: The client instance for this callback.
            userdata: The private user data as set in Client() or userdata_set().
            msg: The received MQTTMessage.
        """
        self.log.debug("MQTT message topic=%s payload=%r", msg.topic, msg.payload)
        handler = self._match_handler(msg.topic)
        if handler is None:
            handler = self._user_on_message
        if handler:
            try:
                handler(msg.topic, msg.payload)
            except Exception:
                self.log.exception("Error in message handler for topic %s", msg.topic)

    def _match_handler(self, topic: str) -> Optional[MessageHandler]:
        """
        Return the handler for the *most specific* matching subscription filter.
        If multiple filters match, prefer the most specific (fewest wildcards, longest).

        Args:
            topic: The topic of the incoming message.

        Returns:
            The matched handler, or None if no match.
        """
        best_filter = None
        best_score = (-1, -1)
        with self._lock:
            for filt, h in self._routes.items():
                if topic_matches_sub(filt, topic):
                    non_wildcards = sum(
                        1 for part in filt.split("/") if part not in ("#", "+")
                    )
                    score = (non_wildcards, len(filt))
                    if score > best_score:
                        best_score = score
                        best_filter = filt
            return self._routes.get(best_filter) if best_filter is not None else None

    def set_on_message(self, handler: Optional[MessageHandler]) -> "MQTTClient":
        """Attach/replace the default message handler.

        Args:
            handler: The message handler (topic, payload) or None to clear.

        Returns:
            MQTTClient: Self for chaining.
        """
        with self._lock:
            self._user_on_message = handler
        return self

    def set_on_connect(self, handler: Optional[ConnectHandler]) -> "MQTTClient":
        """Attach/replace the connect handler.

        Args:
            handler: The connect handler () or None to clear.

        Returns:
            MQTTClient: Self for chaining.
        """
        with self._lock:
            self._user_on_connect = handler
        return self

    def set_on_disconnect(self, handler: Optional[DisconnectHandler]) -> "MQTTClient":
        """Attach/replace the disconnect handler.

        Args:
            handler: The disconnect handler (reason_code) or None to clear.

        Returns:
            MQTTClient: Self for chaining.
        """
        with self._lock:
            self._user_on_disconnect = handler
        return self

    def set_logger(self, logger: Optional[logging.Logger]) -> "MQTTClient":
        """Attach/replace the logger.

        Args:
            logger: The logger or None to use a no-op logger.

        Returns:
            MQTTClient: Self for chaining.
        """
        with self._lock:
            if logger is None:
                logger = logging.getLogger(__name__)
                logger.addHandler(logging.NullHandler())
            self.log = logger
        return self


def default_client() -> "MQTTClient":
    """Create an MQTTClient using settings from environment variables or defaults.

    Environment variables:
        MQTT_HOST: MQTT broker hostname or IP (default "localhost").
        MQTT_PORT: MQTT broker port (default 1883).
        MQTT_USER: Optional username for broker authentication.
        MQTT_PASS: Optional password for broker authentication.
        MQTT_TLS_CA: Optional path to CA certificate file for TLS connection.

    Returns:
        MQTTClient: Configured MQTT client instance.
    """

    # Load .env file if present
    parent_dir = os.path.dirname(os.path.dirname(__file__))
    env_path = os.path.join(parent_dir, ".env")
    load_dotenv(dotenv_path=env_path)
    MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_USER = os.getenv("MQTT_USER", None)
    MQTT_PASS = os.getenv("MQTT_PASS", None)
    tls_ca_name = os.getenv("MQTT_TLS_CA")
    MQTT_TLS_CA = (
        os.path.join(os.path.dirname(sys.argv[0]), "certs", tls_ca_name)
        if tls_ca_name
        else None
    )

    # Return MQTT client
    return MQTTClient(
        host=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASS,
        tls_ca=MQTT_TLS_CA,
    )
