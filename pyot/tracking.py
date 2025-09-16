import json
import logging
import os
import socket
import time
from datetime import datetime

import psutil

from pyot.config import AppConfig
from pyot.mqtt import MQTTClient


class Tracker:
    """Class for tracking and publishing heartbeat and version information."""

    def __init__(
        self, config: AppConfig, client: MQTTClient, log: logging.Logger
    ) -> None:
        """Initialize tracker.

        Stores references to config, MQTT client, and app logger.
        Retrieves basic information and initializes tracking variables.

        Args:
            config: Application configuration.
            client: MQTT client instance.
            log: Logger instance.
        """

        # Store references to config, MQTT client, and app logger.
        self.config = config
        self.client = client
        self.log = log

        # Retrieve basic information.
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.process = psutil.Process(self.pid)

        # Initialize tracking variables.
        self.process_start = time.time()
        self.last_heartbeat = 0

    def track(self) -> None:
        """Track and publish heartbeat.

        This method checks if it's time to send a heartbeat. If so, it gathers
        system and process information, constructs a payload, and publishes it
        to the MQTT broker.
        """

        now = time.time()

        # Publish heartbeat if interval has passed.
        if now - self.last_heartbeat >= self.config.HEARTBEAT_INTERVAL:
            self.last_heartbeat = now

            # Gather system and process information.
            uptime = int(now - self.process_start)
            memory = self.process.memory_info().rss

            # Construct payload.
            payload = json.dumps(
                {
                    "hostname": self.hostname,
                    "timestamp": datetime.now().isoformat(),
                    "uptime": uptime,
                    "memory": memory,
                    "pid": self.pid,
                    "version": self.config.CURRENT_VERSION,
                }
            )

            # Publish heartbeat payload.
            self.client.publish(
                f"{self.config.HEARTBEAT_TOPIC}/{self.hostname}",
                payload,
                qos=self.config.HEARTBEAT_QOS,
                retain=self.config.HEARTBEAT_RETAIN,
            )
            self.log.debug(f"Published heartbeat: {payload}")
