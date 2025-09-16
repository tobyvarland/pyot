import json
import os
import socket
import time
from datetime import datetime

import psutil

from pyot.config import get_settings
from pyot.handler import (
    BaseHandler,
    LogAnnualizationHandler,
    PushToServerHandler,
    SyncShopOrderRecipesHandler,
)
from pyot.logging import setup_logger
from pyot.mqtt import default_client

# Load configuration
config = get_settings()

# Set up logger
log = setup_logger(level=config.log_level)


def main() -> None:
    """Main function to set up MQTT client, configure logging, and subscribe to topics."""

    # Create client
    client = default_client(config.broker)

    # Configure logger for MQTT client and handlers
    client.set_logger(log)
    BaseHandler.set_logger(log)

    # Configure handlers with necessary settings
    SyncShopOrderRecipesHandler.set_config(config.pull_shop_orders)
    PushToServerHandler.set_config(config.push_to_server)
    LogAnnualizationHandler.set_config(config.annualize_logs)

    # Subscribe to topics with appropriate handlers
    client.subscribe(config.push_to_server.TOPIC, handler=PushToServerHandler.handle)
    client.subscribe(
        config.annualize_logs.TOPIC, handler=LogAnnualizationHandler.handle
    )
    if config.pull_shop_orders.pull:
        client.subscribe(
            config.pull_shop_orders.TOPIC, handler=SyncShopOrderRecipesHandler.handle
        )

    # Start client and run until interrupted
    try:
        client.start()
        log.debug("Press Ctrl+C to exit")
        last_heartbeat = 0
        last_version = None
        version_count = 0
        heartbeat_count = 0
        process_start = time.time()
        hostname = socket.gethostname()
        while True:
            now = time.time()
            current_dt = datetime.now()
            today = current_dt.date()
            if today != last_version:
                log.debug("Publishing version")
                version_count += 1
                payload = json.dumps(
                    {
                        "hostname": hostname,
                        "timestamp": current_dt.isoformat(),
                        "version": config.CURRENT_VERSION,
                        "version_count": version_count,
                    }
                )
                client.publish(
                    f"pyot/version/{hostname}",
                    payload,
                    qos=1,
                    retain=True,
                )
                last_version = today
            if now - last_heartbeat >= config.HEARTBEAT_INTERVAL:
                log.debug("Publishing heartbeat")
                uptime = int(now - process_start)
                pid = os.getpid()
                memory = psutil.Process(pid).memory_info().rss
                heartbeat_count += 1
                payload = json.dumps(
                    {
                        "hostname": hostname,
                        "timestamp": current_dt.isoformat(),
                        "heartbeat_count": heartbeat_count,
                        "uptime": uptime,
                        "memory": memory,
                        "pid": pid,
                    }
                )
                client.publish(
                    f"pyot/heartbeat/{hostname}",
                    payload,
                    qos=1,
                    retain=True,
                )
                last_heartbeat = now
            time.sleep(1)
    except KeyboardInterrupt:
        log.debug("Shutting down")
    finally:
        client.stop()


# Run main if executed as script
if __name__ == "__main__":
    main()
