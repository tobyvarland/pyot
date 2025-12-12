import logging
import os
import shlex
import shutil
import socket
import subprocess
from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from pydantic import BaseModel, Field, ValidationError

from pyot.config import (
    AnnualizeLogsConfig,
    AuthRecipeWriterConfig,
    PullShopOrdersConfig,
    PushToServerConfig,
)


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

    Attributes:
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
            cls.logger.debug("PushToServerHandler: including log processing steps")
            cls.logger.debug(
                f"PushToServerHandler: log folder name: {config.log_folder_name}"
            )
        else:
            cls.logger.debug("PushToServerHandler: skipping log processing steps")

    @classmethod
    def handle(cls, topic: str, payload: bytes) -> None:
        """Base message handler.

        Receives message and uses short circuit evaluation to call additional methods.
        Payload is ignored.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """

        # Log receipt of message
        cls.logger.debug("PushToServerHandler: message received on topic: %s", topic)

        # Call methods using short circuit evaluation
        steps = [cls._create_data_directory, cls._push_to_server]
        if cls.config.centralize_logs:
            steps.extend([cls._create_log_directory, cls._copy_logs])
        if all(f() for f in steps):
            cls.logger.debug("PushToServerHandler: all handlers successful")
        else:
            cls.logger.warning("PushToServerHandler: handler failed")

    @classmethod
    def _create_data_directory(cls) -> bool:
        """Create data directory on remote server if it does not exist.

        Executes mkdir via SSH on remote server. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("PushToServerHandler: ensuring data directory exists on server")
        cmd = ["wsl"] if cls.config.use_wsl else []
        cmd.extend(
            [
                "/usr/bin/ssh",
                "-o",
                "StrictHostKeyChecking=accept-new",
                cls.config.remote_server,
                "/bin/mkdir",
                "-p",
                f"{cls.config.remote_path}{socket.gethostname()}/",
            ]
        )
        cls.logger.debug(
            "PushToServerHandler: starting remote mkdir: %s", shlex.join(cmd)
        )
        try:
            subprocess.check_call(cmd)
            cls.logger.debug("PushToServerHandler: mkdir successful")
            cls._create_data_directory = lambda *args, **kwargs: True
            return True
        except Exception:
            cls.logger.exception("PushToServerHandler: mkdir failed")
            return False

    @classmethod
    def _push_to_server(cls) -> bool:
        """Push PLCData folder to remote server.

        Uses WSL to execute rsync command. SSH key authentication must be set up.

        Skips chart files for the current day to avoid versions in backup.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("PushToServerHandler: pushing local data to server")
        today = date.today().strftime("%y%m%d")
        cmd = ["wsl"] if cls.config.use_wsl else []
        cmd.extend(
            [
                "rsync",
                "-rt",
                "--delete",
                "--delete-excluded",
                "-e",
                "ssh -o StrictHostKeyChecking=accept-new",
                f"--exclude=Charts/RD{today}.*",
                cls.config.local_path,
                f"{cls.config.remote_server}:{cls.config.remote_path}{socket.gethostname()}/",
            ]
        )
        cls.logger.debug("PushToServerHandler: starting rsync: %s", shlex.join(cmd))
        try:
            subprocess.check_call(cmd)
            cls.logger.debug("PushToServerHandler: rsync successful")
            return True
        except Exception:
            cls.logger.exception("PushToServerHandler: rsync failed")
            return False

    @classmethod
    def _create_log_directory(cls) -> bool:
        """Create logs directory on remote server if it does not exist.

        Executes mkdir via SSH on remote server. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("PushToServerHandler: ensuring logs directory exists on server")
        cmd = ["wsl"] if cls.config.use_wsl else []
        cmd.extend(
            [
                "/usr/bin/ssh",
                "-o",
                "StrictHostKeyChecking=accept-new",
                cls.config.remote_server,
                "/bin/mkdir",
                "-p",
                f"{cls.config.remote_log_path}{cls.config.log_folder_name}/",
            ]
        )
        cls.logger.debug(
            "PushToServerHandler: starting remote mkdir: %s", shlex.join(cmd)
        )
        try:
            subprocess.check_call(cmd)
            cls.logger.debug("PushToServerHandler: mkdir successful")
            cls._create_log_directory = lambda *args, **kwargs: True
            return True
        except Exception:
            cls.logger.exception("PushToServerHandler: mkdir failed")
            return False

    @classmethod
    def _copy_logs(cls) -> bool:
        """Copy logs from backup folder to user accessible folder on remote server.

        Executes rsync via SSH on remote server. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("PushToServerHandler: copying logs to user accessible folder")
        cmd = ["wsl"] if cls.config.use_wsl else []
        rsync_args = [
            "/bin/rsync",
            "-a",
        ]
        if not cls.config.merge_logs:
            rsync_args.append("--delete")
        cmd.extend(
            [
                "/usr/bin/ssh",
                "-o",
                "StrictHostKeyChecking=accept-new",
                cls.config.remote_server,
                *rsync_args,
                f"{cls.config.remote_path}{socket.gethostname()}/Logs/",
                f"{cls.config.remote_log_path}{cls.config.log_folder_name}/",
            ]
        )
        cls.logger.debug(
            "PushToServerHandler: starting remote rsync: %s", shlex.join(cmd)
        )
        try:
            subprocess.check_call(cmd)
            cls.logger.debug("PushToServerHandler: rsync successful")
            return True
        except Exception:
            cls.logger.exception("PushToServerHandler: rsync failed")
            return False


class SyncShopOrderRecipesHandler(BaseHandler):
    """Handler for 'as400/shop_order_recipes_synced' topic.

    Upon receiving message, uses WSL to execute rsync to copy shop order recipes
    from remote server to local directory. SSH key authentication must be set up.

    Attributes:
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

        Receives message and uses short circuit evaluation to call additional methods.
        Payload is ignored.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """

        # Log receipt of message
        cls.logger.debug(
            "SyncShopOrderRecipesHandler: message received on topic: %s", topic
        )

        # Call methods using short circuit evaluation
        if all(f() for f in (cls._pull_from_server,)):
            cls.logger.debug("SyncShopOrderRecipesHandler: all handlers successful")
        else:
            cls.logger.warning("SyncShopOrderRecipesHandler: handler failed")

    @classmethod
    def _pull_from_server(cls) -> bool:
        """Pull shop order recipes from remote server.

        Uses WSL to execute rsync command. SSH key authentication must be set up.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("SyncShopOrderRecipesHandler: pulling recipes from server")
        cmd = ["wsl"] if cls.config.use_wsl else []
        cmd.extend(
            [
                "rsync",
                "-rt",
                "--checksum",
                "--delete",
                "--omit-dir-times",
                "-e",
                "ssh -o Compression=no -o StrictHostKeyChecking=accept-new",
                f"{cls.config.remote_server}:{cls.config.remote_path}",
                cls.config.local_path,
            ]
        )
        cls.logger.debug(
            "SyncShopOrderRecipesHandler: starting rsync: %s", shlex.join(cmd)
        )
        try:
            subprocess.check_call(cmd)
            cls.logger.debug("SyncShopOrderRecipesHandler: rsync successful")
            return True
        except Exception:
            cls.logger.exception("SyncShopOrderRecipesHandler: rsync failed")
            return False


class LogAnnualizationHandler(BaseHandler):
    """Handler for 'plc/annualize_logs' topic.

    Copies all .csv log files from the current log directory to an annualized
    directory structure based on the year.

    Attributes:
        config (AnnualizeLogsConfig): Configuration for the handler.
    """

    config: AnnualizeLogsConfig

    @classmethod
    def set_config(cls, config: AnnualizeLogsConfig) -> None:
        """Set config for the handler.

        Args:
            config (AnnualizeLogsConfig): Configuration to set.
        """
        cls.config = config

    @classmethod
    def handle(cls, topic: str, payload: bytes) -> None:
        """Base message handler.

        Receives message and uses short circuit evaluation to call additional methods.
        Payload is ignored.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """

        # Log receipt of message
        cls.logger.debug(
            "LogAnnualizationHandler: message received on topic: %s", topic
        )

        # Call methods using short circuit evaluation
        if all(f() for f in (cls._annualize_logs,)):
            cls.logger.debug("LogAnnualizationHandler: all handlers successful")
        else:
            cls.logger.warning("LogAnnualizationHandler: handler failed")

    @classmethod
    def _annualize_logs(cls) -> bool:
        """Copies log files to annualized directory structure.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("LogAnnualizationHandler: copying logs to annualized folder")
        try:
            directory = Path(cls.config.logs_directory)
            files = list(directory.glob("*.csv"))
            if not files:
                cls.logger.debug("LogAnnualizationHandler: no .csv files to annualize")
                return True
            tz = ZoneInfo("America/New_York")
            today = datetime.now(tz).date()
            target_year = (
                today.year - 1 if (today.month, today.day) == (1, 1) else today.year
            )
            target_directory = directory / str(target_year)
            target_directory.mkdir(exist_ok=True)
            for file in files:
                shutil.move(file, target_directory / file.name)
            cls.logger.debug(
                "LogAnnualizationHandler: moved %d .csv files to %s",
                len(files),
                target_directory,
            )
            return True
        except Exception:
            cls.logger.exception("LogAnnualizationHandler: copying log files failed")
            return False


class AuthRecipeHandler(BaseHandler):
    """Handler for 'plc/refresh_auth' topic.

    Writes auth recipes to the specified directory.

    Attributes:
        config (AuthRecipeWriterConfig): Configuration for the handler.
    """

    config: AuthRecipeWriterConfig

    class Employee(BaseModel):
        employee_number: int = Field(gt=1, lt=1000)
        employee_name: str
        user_pin: str

    @classmethod
    def set_config(cls, config: AuthRecipeWriterConfig) -> None:
        """Set config for the handler.

        Args:
            config (AuthRecipeWriterConfig): Configuration to set.
        """
        cls.config = config

    @classmethod
    def handle(cls, topic: str, payload: bytes) -> None:
        """Base message handler.

        Receives message and uses short circuit evaluation to call additional methods.
        Payload is ignored.

        Args:
            topic (str): The MQTT topic of the message.
            payload (bytes): The payload of the message.
        """

        # Log receipt of message
        cls.logger.debug("AuthRecipeHandler: message received on topic: %s", topic)

        # Call methods using short circuit evaluation
        if all(f() for f in (cls._create_local_folder, cls._write_recipe_file)):
            cls.logger.debug("AuthRecipeHandler: all handlers successful")
        else:
            cls.logger.warning("AuthRecipeHandler: handler failed")

    @classmethod
    def _create_local_folder(cls) -> bool:
        """Ensures local folder for auth recipes exists.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("AuthRecipeHandler: ensuring local folder exists")
        try:
            os.makedirs(cls.config.folder, exist_ok=True)
            cls.logger.debug("AuthRecipeHandler: created/verified local directory")
            cls._create_local_folder = lambda *args, **kwargs: True
            return True
        except Exception:
            cls.logger.exception(
                "AuthRecipeHandler: ensuring local directory existence failed"
            )
            return False

    @classmethod
    def _write_recipe_file(cls) -> bool:
        """Writes auth recipe file.

        Returns:
            bool: True if successful, False otherwise.
        """
        cls.logger.info("AuthRecipeHandler: writing recipe file")
        try:
            employees = cls._fetch_employees()
            path = os.path.join(cls.config.folder, cls.config.filename)
            with open(path, "w") as f:
                for controller in cls.config.controllers:
                    f.write(f"{controller}:String Table.{cls.config.PIN_TABLE_NAME}\n")
                    for emp in employees:
                        f.write(f"{emp.employee_number}:{emp.user_pin}\n")
                    f.write("\n")
                    f.write(f"{controller}:String Table.{cls.config.NAME_TABLE_NAME}\n")
                    for emp in employees:
                        f.write(f"{emp.employee_number}:{emp.employee_name}\n")
                    f.write("\n")
                f.write("\n")
            cls.logger.debug("AuthRecipeHandler: wrote auth recipe")
            return True
        except Exception:
            cls.logger.exception("AuthRecipeHandler: writing recipe file failed")
            return False

    @classmethod
    def _fetch_employees(cls) -> list[Employee]:
        """Fetches employee data from the API.

        Returns:
            list[Employee]: List of employee data dictionaries.
        """
        try:
            employees = []
            response = requests.get(cls.config.API_ENDPOINT, timeout=10)
            response.raise_for_status()
            raw_employees = response.json()
            for item in raw_employees:
                try:
                    employees.append(cls.Employee.model_validate(item))
                except ValidationError:
                    pass
            cls.logger.debug(
                "AuthRecipeHandler: fetched %d employees from API", len(employees)
            )
            return employees
        except Exception:
            cls.logger.exception(
                "AuthRecipeHandler: fetching employees from API failed"
            )
            return []
