from __future__ import annotations

import logging
import os
import socket
import ssl
import sys
import threading
import uuid
from typing import Callable, Optional, Union

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

MessageHandler = Callable[[str, bytes], None]
ConnectHandler = Callable[[], None]
DisconnectHandler = Callable[[int], None]


class MQTTClient:
    """
    A small wrapper around paho-mqtt that:
      - connects and subscribes to a topic
      - invokes optional callbacks for message, connect, and disconnect
      - integrates with an external logging.Logger
    """

    def __init__(
        self,
        host: str,
        port: int = 1883,
        topic: str = "#",
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

        # Store connection params
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.qos = qos
        self._topic = topic

        # External logger or a no-op logger
        self.log = logger or logging.getLogger(__name__)
        if not logger:
            self.log.addHandler(logging.NullHandler())

        # Store callbacks (all optional)
        self._user_on_message = on_message
        self._user_on_connect = on_connect
        self._user_on_disconnect = on_disconnect

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

        self._loop_running = False
        self._lock = threading.RLock()

    # ---------- Public API ----------

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
    ):
        """
        Convenience publish. Returns a MQTTMessageInfo object.
        """
        q = self.qos if qos is None else qos
        self.log.debug("MQTT publish topic=%s qos=%s retain=%s", topic, q, retain)
        return self._client.publish(topic, payload=payload, qos=q, retain=retain)

    # ---------- Internal handlers (delegate to user callbacks) ----------

    def _on_connect(self, client: mqtt.Client, userdata, flags, rc, *args):
        if rc == 0:
            self.log.info("MQTT connected.")
            self.log.info("MQTT subscribing to %s (qos=%d)", self._topic, self.qos)
            result, mid = client.subscribe(self._topic, qos=self.qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                self.log.error(
                    "Failed to subscribe to %s: code=%s", self._topic, result
                )
        else:
            self.log.warning("MQTT connect returned non-zero result: rc=%s", rc)
        if self._user_on_connect:
            try:
                self._user_on_connect()
            except Exception:
                self.log.exception("Error in user on_connect handler")

    def _on_disconnect(self, client: mqtt.Client, userdata, rc, *args):
        if rc != 0:
            self.log.warning(
                "MQTT unexpectedly disconnected (rc=%s). Will auto-reconnect.", rc
            )
        else:
            self.log.info("MQTT cleanly disconnected.")
        if self._user_on_disconnect:
            try:
                self._user_on_disconnect(rc)
            except Exception:
                self.log.exception("Error in user on_disconnect handler")

    def _on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        self.log.debug("MQTT message topic=%s payload=%r", msg.topic, msg.payload)
        if self._user_on_message:
            try:
                self._user_on_message(msg.topic, msg.payload)
            except Exception:
                self.log.exception("Error in user on_message handler")

    def set_on_message(self, handler: Optional[MessageHandler]) -> "MQTTClient":
        """Attach/replace the message handler. Pass None to clear."""
        with self._lock:
            self._user_on_message = handler
        return self

    def set_on_connect(self, handler: Optional[ConnectHandler]) -> "MQTTClient":
        """Attach/replace the connect handler. Pass None to clear."""
        with self._lock:
            self._user_on_connect = handler
        return self

    def set_on_disconnect(self, handler: Optional[DisconnectHandler]) -> "MQTTClient":
        """Attach/replace the disconnect handler. Pass None to clear."""
        with self._lock:
            self._user_on_disconnect = handler
        return self

    def set_logger(self, logger: Optional[logging.Logger]) -> "MQTTClient":
        """
        Attach/replace the logger. If None, use a module-level logger with NullHandler.
        """
        with self._lock:
            if logger is None:
                logger = logging.getLogger(__name__)
                logger.addHandler(logging.NullHandler())
            self.log = logger
        return self

    @property
    def on_message(self) -> Optional[MessageHandler]:
        return self._user_on_message

    @on_message.setter
    def on_message(self, handler: Optional[MessageHandler]) -> None:
        with self._lock:
            self._user_on_message = handler

    @property
    def on_connect(self) -> Optional[ConnectHandler]:
        return self._user_on_connect

    @on_connect.setter
    def on_connect(self, handler: Optional[ConnectHandler]) -> None:
        with self._lock:
            self._user_on_connect = handler

    @property
    def on_disconnect(self) -> Optional[DisconnectHandler]:
        return self._user_on_disconnect

    @on_disconnect.setter
    def on_disconnect(self, handler: Optional[DisconnectHandler]) -> None:
        with self._lock:
            self._user_on_disconnect = handler

    @property
    def logger(self) -> logging.Logger:
        """Return the current logger."""
        return self.log

    @logger.setter
    def logger(self, logger: Optional[logging.Logger]) -> None:
        with self._lock:
            if logger is None:
                logger = logging.getLogger(__name__)
                logger.addHandler(logging.NullHandler())
            self.log = logger


def default_client(topic: str = "#") -> "MQTTClient":

    # Load .env file if present
    parent_dir = os.path.dirname(os.path.dirname(__file__))
    env_path = os.path.join(parent_dir, ".env")
    load_dotenv(dotenv_path=env_path)
    MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_USER = os.getenv("MQTT_USER", None)
    MQTT_PASS = os.getenv("MQTT_PASS", None)
    MQTT_TLS_CA = (
        f"{os.path.join(os.path.dirname(sys.argv[0]), "certs")}/{os.getenv("MQTT_TLS_CA")}"
        if os.getenv("MQTT_TLS_CA")
        else None
    )

    # Return MQTT client
    return MQTTClient(
        host=MQTT_HOST,
        port=MQTT_PORT,
        topic=topic,
        username=MQTT_USER,
        password=MQTT_PASS,
        tls_ca=MQTT_TLS_CA,
    )
