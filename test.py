import logging
import os
import time

from dotenv import load_dotenv

from pyot.handler import BaseHandler, PushToServerHandler, SyncShopOrderRecipesHandler
from pyot.logging import setup_logger
from pyot.mqtt import default_client

# Set up logger
log = setup_logger(level=logging.INFO)


def env_bool(value: str) -> bool:
    """Convert environment variable string to boolean.

    Args:
        value (str): Environment variable string.

    Returns:
        bool: Converted boolean value.
    """
    return value.lower() in ("1", "true", "yes", "on")


def main() -> None:
    """Main function to set up MQTT client, configure logging, and subscribe to topics."""

    # Load .env
    script_dir = os.path.dirname(__file__)
    env_path = os.path.join(script_dir, ".env")
    load_dotenv(dotenv_path=env_path)
    PUSH_TO_SERVER_MQTT_TOPIC = os.getenv("PUSH_TO_SERVER_MQTT_TOPIC", "#")
    PUSH_TO_SERVER_INCLUDE_LOGS = env_bool(
        os.getenv("PUSH_TO_SERVER_INCLUDE_LOGS", "true")
    )
    PULL_SHOP_ORDERS_MQTT_TOPIC = os.getenv("PULL_SHOP_ORDERS_MQTT_TOPIC", "#")
    PULL_SHOP_ORDERS = env_bool(os.getenv("PULL_SHOP_ORDERS", "false"))

    # Create client
    client = default_client()

    # Configure logger for MQTT client and handlers
    client.set_logger(log)
    BaseHandler.set_logger(log)

    # Configure PushToServerHandler to process logs based on env variable
    PushToServerHandler.set_process_logs(PUSH_TO_SERVER_INCLUDE_LOGS)

    # Subscribe to topics with appropriate handlers
    client.subscribe(PUSH_TO_SERVER_MQTT_TOPIC, handler=PushToServerHandler.handle)
    if PULL_SHOP_ORDERS:
        client.subscribe(
            PULL_SHOP_ORDERS_MQTT_TOPIC, handler=SyncShopOrderRecipesHandler.handle
        )

    # Start client and run until interrupted
    try:
        client.start()
        log.info("Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        client.stop()


# Run main if executed as script
if __name__ == "__main__":
    main()
