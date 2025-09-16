import time

from pyot.config import get_settings
from pyot.handler import (
    AuthRecipeHandler,
    BaseHandler,
    LogAnnualizationHandler,
    PushToServerHandler,
    SyncShopOrderRecipesHandler,
)
from pyot.logging import setup_logger
from pyot.mqtt import default_client
from pyot.tracking import Tracker

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

    # Setup process tracker.
    tracker = Tracker(config, client, log)

    # Configure handlers with necessary settings
    SyncShopOrderRecipesHandler.set_config(config.pull_shop_orders)
    PushToServerHandler.set_config(config.push_to_server)
    LogAnnualizationHandler.set_config(config.annualize_logs)
    AuthRecipeHandler.set_config(config.auth_recipe_writer)

    # Subscribe to topics with appropriate handlers
    client.subscribe(config.push_to_server.TOPIC, handler=PushToServerHandler.handle)
    client.subscribe(
        config.annualize_logs.TOPIC, handler=LogAnnualizationHandler.handle
    )
    if config.pull_shop_orders.pull:
        client.subscribe(
            config.pull_shop_orders.TOPIC, handler=SyncShopOrderRecipesHandler.handle
        )
    if config.auth_recipe_writer.create:
        client.subscribe(
            config.auth_recipe_writer.TOPIC, handler=AuthRecipeHandler.handle
        )

    # Start client and run until interrupted
    try:
        client.start()
        log.debug("Press Ctrl+C to exit")
        while True:
            tracker.track()
            time.sleep(config.SLEEP_INTERVAL)
    except KeyboardInterrupt:
        log.debug("Shutting down")
    finally:
        client.stop()


# Run main if executed as script
if __name__ == "__main__":
    main()
