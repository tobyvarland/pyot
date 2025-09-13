import logging
import os
import sys
from datetime import time as dtime
from logging.handlers import TimedRotatingFileHandler


def setup_logger(
    name: str = "pyot",
    level: int = logging.INFO,
    logs_dir: str = "logs",
    *,
    backup_count: int = 7,
    use_utc: bool = False,
    at_time: dtime | None = None,
) -> logging.Logger:
    """Confifure logger instance.

    Configure a logger that writes to logs/<scriptname>.log,
    rotates daily at midnight, and keeps 'backup_count' days.
    Console output happens only when level == DEBUG.

    Args:
        name: Logger name.
        level: Logging level.
        logs_dir: Directory to store log files.
        backup_count: Number of days to keep log files.
        use_utc: Use UTC time for log rotation.
        at_time: Specific time to rotate logs (default is midnight).

    Returns:
        logging.Logger: Configured logger instance.
    """

    # Determine log file path based on script name and location
    script_path = sys.argv[0]
    script_name = os.path.splitext(os.path.basename(script_path))[0]
    log_dir = os.path.join(os.path.dirname(script_path), logs_dir)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{script_name}.log")

    # Make a dedicated logger and keep it independent
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # Clear handlers to avoid duplicates if necessary
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
            h.close()

    # Log message format
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Configure file handler with rotation
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        interval=1,
        backupCount=backup_count,
        utc=use_utc,
        atTime=at_time,
        encoding="utf-8",
        delay=True,
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)

    # Configure console handler for DEBUG level
    if level == logging.DEBUG:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(fmt)
        console.setLevel(logging.DEBUG)
        logger.addHandler(console)

    # Return the configured logger
    return logger
