from __future__ import annotations

import ipaddress
import logging
import os
import socket
from dataclasses import dataclass
from datetime import timedelta
from functools import lru_cache
from typing import ClassVar, List, Optional

from dotenv import load_dotenv

# Load environment from .env file
try:
    parent_dir = os.path.dirname(os.path.dirname(__file__))
    env_path = os.path.join(parent_dir, ".env")
    load_dotenv(dotenv_path=env_path)
except Exception:
    pass


class _MissingEnv(Exception):
    """Exception raised when a required environment variable is missing or empty."""

    pass


def _get_required(name: str) -> str:
    """Get required environment variable or raise if missing/empty.

    Args:
        name (str): Environment variable name.

    Returns:
        str: Environment variable value.

    Raises:
        _MissingEnv: If the environment variable is missing or empty."""
    val = os.getenv(name)
    if val is None or val == "":
        raise _MissingEnv(f"Required environment variable {name!r} is missing/empty.")
    return val


def _get(name: str, default: Optional[str] = None) -> str:
    """Get optional environment variable or default if missing.

    Args:
        name (str): Environment variable name.
        default (Optional[str], optional): Default value if missing. Defaults to None.

    Returns:
        Optional[str]: Environment variable value or default.
    """
    if default is None:
        default = ""
    return os.getenv(name, default)


def _to_bool(value: Optional[str] = None) -> bool:
    """Convert string to boolean.

    Args:
        value (str): Input string.

    Returns:
        bool: Converted boolean value.
    """
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _to_int(value: Optional[str] = None) -> int:
    """Convert string to integer, allowing underscores.

    Args:
        value (str): Input string.

    Returns:
        int: Converted integer value.
    """
    if value is None:
        return 0
    return int(value.replace("_", ""))


def _to_list(value: str, sep: str = ",") -> List[str]:
    """Convert comma-separated string to list of strings.

    Args:
        value (str): Input string.
        sep (str, optional): Separator character. Defaults to ",".

    Returns:
        List[str]: List of strings.
    """
    return [item.strip() for item in value.split(sep) if item.strip()]


def _to_seconds(value: str) -> int:
    """
    Parse '30s', '5m', '2h', '1d' → seconds. If plain int, treat as seconds.

    Args:
        value (str): Input string.

    Returns:
        int: Number of seconds.
    """
    s = value.strip().lower()
    if s.endswith("s"):
        return int(s[:-1])
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return int(s[:-1]) * 86400
    return int(s)


def _to_timedelta(value: str) -> timedelta:
    """Convert string to timedelta.

    Args:
        value (str): Input string.

    Returns:
        timedelta: Converted timedelta value.
    """
    return timedelta(seconds=_to_seconds(value))


def _to_ip(value: str):
    """Validate and convert string to IP address.

    Args:
        value (str): Input string.

    Returns:
        ipaddress.IPv4Address | ipaddress.IPv6Address: Converted IP address.

    Raises:
        ValueError: If the string is not a valid IP address.
    """
    return ipaddress.ip_address(value)


@dataclass(frozen=True)
class BrokerConfig:
    """MQTT Broker configuration.

    Atributes:
        host (str): Broker hostname or IP address.
        port (int): Broker port number.
        tls_ca (Optional[str]): Path to TLS CA certificate file.
        username (Optional[str]): Username for broker authentication.
        password (Optional[str]): Password for broker authentication.
    """

    host: str
    port: int
    tls_ca: Optional[str]
    username: Optional[str]
    password: Optional[str]


@dataclass(frozen=True)
class PullShopOrdersConfig:
    """Config for pulling shop orders from System i.

    Atributes:
        pull (bool): Whether to pull shop orders.
        remote_server (str): Remote server SSH user and hostname.
        remote_path (str): Remote path to pull shop orders from.
        local_path (str): Local path to store pulled shop orders.
        use_wsl (bool): Whether to use WSL for rsync command.

        TOPIC (ClassVar[str]): MQTT topic for shop order recipes.
    """

    pull: bool
    remote_server: str
    remote_path: str
    local_path: str
    use_wsl: bool
    TOPIC: ClassVar[str] = "as400/shop_order_recipes_synced"


@dataclass(frozen=True)
class PushToServerConfig:
    """Config for pushing local data to central server.

    Atributes:
        centralize_logs (bool): Whether to centralize logs.
        log_folder_name (str): Folder name for storing logs.
        use_wsl (bool): Whether to use WSL for rsync command.
        remote_server (str): Remote server SSH user and hostname.
        remote_path (str): Remote path to push data to.
        remote_log_path (str): Remote path to push logs to.
        local_path (str): Local path to push data from.

        TOPIC (ClassVar[str]): MQTT topic for shop order recipes.
    """

    centralize_logs: bool
    log_folder_name: str
    use_wsl: bool
    remote_server: str
    remote_path: str
    remote_log_path: str
    local_path: str
    TOPIC: ClassVar[str] = "plc/push_to_server"


@dataclass(frozen=True)
class AnnualizeLogsConfig:
    """Config for annualizing log files.

    Atributes:
        logs_directory (str): Directory where logs are stored.
        TOPIC (ClassVar[str]): MQTT topic for triggering annualization.
    """

    logs_directory: str
    TOPIC: ClassVar[str] = "plc/annualize_logs"


@dataclass(frozen=True)
class AppConfig:
    """Application configuration loaded from environment variables.

    Attributes:
        log_level (int): Logging level.
        broker (BrokerConfig): MQTT Broker configuration.
        pull_shop_orders (PullShopOrdersConfig): Shop orders pulling configuration.
        push_to_server (PushToServerConfig): Push to server configuration.
        annualize_logs (AnnualizeLogsConfig): Annualize logs configuration.

        CURRENT_VERSION (ClassVar[str]): Current application version.
        HEARTBEAT_INTERVAL (ClassVar[int]): Heartbeat interval in seconds.
    """

    log_level: int
    broker: BrokerConfig
    pull_shop_orders: PullShopOrdersConfig
    push_to_server: PushToServerConfig
    annualize_logs: AnnualizeLogsConfig

    CURRENT_VERSION: ClassVar[str] = "0.0.2"
    HEARTBEAT_INTERVAL: ClassVar[int] = 30

    @staticmethod
    def from_env() -> "AppConfig":
        """Load configuration from environment variables.

        Returns:
            AppConfig: Loaded configuration instance.

        Raises:
            _MissingEnv: If a required environment variable is missing.
        """

        log_level = (
            logging.DEBUG
            if _to_bool(_get("LOG_LEVEL_DEBUG", "false"))
            else logging.INFO
        )

        mqtt_host = _get_required("MQTT_HOST")
        mqtt_port = _to_int(_get_required("MQTT_PORT"))
        mqtt_username = _get_required("MQTT_USER")
        mqtt_password = _get_required("MQTT_PASS")
        mqtt_ca = _get("MQTT_TLS_CA")

        pull_shop_orders = _to_bool(_get_required("PULL_SHOP_ORDERS"))
        pull_shop_orders_server = _get_required("PULL_SHOP_ORDERS_REMOTE_SERVER")
        pull_shop_orders_remote = _get_required("PULL_SHOP_ORDERS_REMOTE_PATH")
        pull_shop_orders_local = _get_required("PULL_SHOP_ORDERS_LOCAL_PATH")
        pull_shop_orders_wsl = _to_bool(_get_required("PULL_SHOP_ORDERS_USE_WSL"))

        centralize_logs = _to_bool(_get_required("PUSH_TO_SERVER_CENTRALIZE_LOGS"))
        logs_folder_name = _get("PUSH_TO_SERVER_LOG_FOLDER_NAME", socket.gethostname())
        push_to_server_wsl = _to_bool(_get_required("PULL_SHOP_ORDERS_USE_WSL"))
        push_to_server_server = _get_required("PUSH_TO_SERVER_REMOTE_SERVER")
        push_to_server_remote = _get_required("PUSH_TO_SERVER_REMOTE_PATH")
        push_to_server_remote_logs = _get_required("PUSH_TO_SERVER_REMOTE_LOG_PATH")
        push_to_server_local = _get_required("PUSH_TO_SERVER_LOCAL_PATH")

        annualize_logs_directory = _get_required("LOG_ANNUALIZATION_DIRECTORY")

        # Determine full CA path if given
        if mqtt_ca:
            parent_dir = os.path.dirname(os.path.dirname(__file__))
            mqtt_ca = os.path.join(parent_dir, "certs", mqtt_ca)

        return AppConfig(
            log_level=log_level,
            broker=BrokerConfig(
                host=mqtt_host,
                port=mqtt_port,
                tls_ca=mqtt_ca,
                username=mqtt_username,
                password=mqtt_password,
            ),
            pull_shop_orders=PullShopOrdersConfig(
                pull=pull_shop_orders,
                remote_server=pull_shop_orders_server,
                remote_path=pull_shop_orders_remote,
                local_path=pull_shop_orders_local,
                use_wsl=pull_shop_orders_wsl,
            ),
            push_to_server=PushToServerConfig(
                centralize_logs=centralize_logs,
                log_folder_name=logs_folder_name,
                use_wsl=push_to_server_wsl,
                remote_server=push_to_server_server,
                remote_path=push_to_server_remote,
                remote_log_path=push_to_server_remote_logs,
                local_path=push_to_server_local,
            ),
            annualize_logs=AnnualizeLogsConfig(logs_directory=annualize_logs_directory),
        )


# ---------- cached accessor & reload ----------
@lru_cache(maxsize=1)
def get_settings() -> AppConfig:
    """
    Load once and cache. Later imports call this for a cheap read.

    Returns:
        AppConfig: Loaded configuration instance.
    """
    try:
        return AppConfig.from_env()
    except _MissingEnv as e:
        raise RuntimeError(str(e)) from e


def reload_settings() -> AppConfig:
    """
    Clear the cache and re-read environment variables.

    Returns:
        AppConfig: Reloaded configuration instance.
    """
    try:
        parent_dir = os.path.dirname(os.path.dirname(__file__))
        env_path = os.path.join(parent_dir, ".env")
        load_dotenv(dotenv_path=env_path, override=True)
    except Exception:
        pass
    get_settings.cache_clear()
    return get_settings()
