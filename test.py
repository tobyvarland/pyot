import logging
import os
import time

from dotenv import load_dotenv

from farm.logging import setup_logger
from farm.mqtt import default_client

log = setup_logger(level=logging.DEBUG)


# def handle_connect():
#     log.info("Connected! Ready to receive messages.")


# def handle_disconnect(rc: int):
#     log.warning("Disconnected (rc=%s).", rc)


def handle_message(topic: str, payload: bytes):
    log.info("Got message on %s: %s", topic, payload.decode(errors="replace"))


def main():

    # Load .env file if present
    script_dir = os.path.dirname(__file__)
    env_path = os.path.join(script_dir, ".env")
    load_dotenv(dotenv_path=env_path)
    TEST_MQTT_TOPIC = os.getenv("TEST_MQTT_TOPIC", "#")

    # Create client and attach handllers & logger
    client = default_client(topic=TEST_MQTT_TOPIC)
    # client.set_on_connect(handle_connect)
    # client.set_on_disconnect(handle_disconnect)
    client.on_message = handle_message
    client.logger = log

    # Start client
    try:
        client.start()
        log.info("Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
