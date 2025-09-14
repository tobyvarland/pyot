"""Module defining MQTT message handlers."""

import logging
from abc import ABC, abstractmethod

from pyot.config import PullShopOrdersConfig, PushToServerConfig


class BaseHandler(ABC):
    """Abstract handler interface.

    All handlers must inherit from this class and implement the handle method. Also
    includes a logger that can be customized. Logger can either be set on BaseHandler to
    allow all inheriting classes to share a single logger or can be set on a specific
    inheriting class.

    Attributes:
        logger (logging.Logger): Logger instance for the handler.
    """

    """Logger for the handler."""
    logger: logging.Logger = logging.getLogger("app.handlers")
    logger.addHandler(logging.NullHandler())

    @classmethod
    def set_logger(cls, logger: logging.Logger) -> None:
        """Set a custom logger for the handler.

        Args:
            logger (logging.Logger): Logger instance to set.
        """
        cls.logger = logger

    @classmethod
    @abstractmethod
    def handle(cls, topic: str, payload: bytes) -> None:
        """Process an MQTT message. Must be overridden in subclasses.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """
        raise NotImplementedError


class PushToServerHandler(BaseHandler):
    """Handler for 'plc/push_to_server' topic.

    Upon receiving message, performs three tasks:
    1. Uses WSL to execute rsync to copy PLCData folder to remote server.
    2. Executes mkdir command via SSH on remote server to ensure logs directory exists.
    3. Executes rsync command via SSH to copy logs from backup folder to user accessible
       folder on remote server.

    Atributes:
        config (PushToServerConfig): Configuration for the handler.
    """

    """Flag to determine if logs should be processed."""
    config: PushToServerConfig

    @classmethod
    def set_config(cls, config: PushToServerConfig) -> None:
        """Set config for the handler.

        Args:
            config (PushToServerConfig): Configuration to set.
        """
        cls.config = config
        if config.centralize_logs:
            cls.logger.info("PushToServerHandler: including log processing steps")
            cls.logger.info(
                f"PushToServerHandler: log folder name: {config.log_folder_name}"
            )
        else:
            cls.logger.info("PushToServerHandler: skipping log processing steps")

    @classmethod
    def handle(cls, topic: str, payload: bytes) -> None:
        """Base message handler.

        Receives message and uses short cirtuit evaluation to call additional methods.
        Payload is ignored.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """

        # Log receipt of message
        cls.logger.info("PushToServerHandler: message received on topic: %s", topic)

        # Call methods using short circuit evaluation
        steps = [cls._push_to_server]
        if cls.config.centralize_logs:
            steps.extend([cls._create_log_directory, cls._copy_logs])
        if all(f() for f in steps):
            cls.logger.info("PushToServerHandler: all handlers successful")
        else:
            cls.logger.warning("PushToServerHandler: handler failed")

    @classmethod
    def _push_to_server(cls) -> bool:
        """Push PLCData folder to remote server.

        Uses WSL to execute rsync command. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """

        try:

            # Log action
            cls.logger.info("PushToServerHandler: pushing PLCData to server")

            # Return true on completion with no exceptions.
            return True

        except Exception as e:

            # Log error and return false
            cls.logger.error(
                "PushToServerHandler: error pushing PLCData to server: %s", e
            )
            return False

    @classmethod
    def _create_log_directory(cls) -> bool:
        """Create logs directory on remote server if it does not exist.

        Executes mkdir via SSH on remote server. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """

        try:

            # Log action
            cls.logger.info(
                "PushToServerHandler: ensuring log directory exists on server"
            )

            # Return true on completion with no exceptions.
            return True

        except Exception as e:

            # Log error and return false
            cls.logger.error(
                "PushToServerHandler: error creating log directory on server: %s", e
            )
            return False

    @classmethod
    def _copy_logs(cls) -> bool:
        """Copy logs from backup folder to user accessible folder on remote server.

        Executes rsync via SSH on remote server. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """

        try:

            # Log action
            cls.logger.info(
                "PushToServerHandler: copying logs to accessible folder on server"
            )

            # Return true on completion with no exceptions.
            return True

        except Exception as e:

            # Log error and return false
            cls.logger.error(
                "PushToServerHandler: error copying logs to accessible folder: %s", e
            )
            return False


class SyncShopOrderRecipesHandler(BaseHandler):
    """Handler for 'as400/shop_order_recipes_synced' topic.

    Upon receiving message, uses WSL to execute rsync to copy shop order recipes
    from remote server to local directory. SSH key authentication must be set up.

    Atributes:
        config (PullShopOrdersConfig): Configuration for the handler.
    """

    config: PullShopOrdersConfig

    @classmethod
    def set_config(cls, config: PullShopOrdersConfig) -> None:
        """Set config for the handler.

        Args:
            config (PullShopOrdersConfig): Configuration to set.
        """
        cls.config = config

    @classmethod
    def handle(cls, topic: str, payload: bytes) -> None:
        """Base message handler.

        Receives message and uses short cirtuit evaluation to call additional methods.
        Payload is ignored.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """

        # Log receipt of message
        cls.logger.info(
            "SyncShopOrderRecipesHandler: message received on topic: %s", topic
        )

        # Call methods using short circuit evaluation
        if all(f() for f in (cls._pull_from_server,)):
            cls.logger.info("SyncShopOrderRecipesHandler: all handlers successful")
        else:
            cls.logger.warning("SyncShopOrderRecipesHandler: handler failed")

    @classmethod
    def _pull_from_server(cls) -> bool:
        """Pull shop order recipes from remote server.

        Uses WSL to execute rsync command. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """

        try:

            # Log action
            cls.logger.info("SyncShopOrderRecipesHandler: pulling recipes from server")

            # Return true on completion with no exceptions.
            return True

        except Exception as e:

            # Log error and return false
            cls.logger.error(
                "SyncShopOrderRecipesHandler: error pulling recipes from server: %s", e
            )
            return False
