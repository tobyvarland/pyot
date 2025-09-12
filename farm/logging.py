import logging
import os
import sys
from datetime import time as dtime
from logging.handlers import TimedRotatingFileHandler


def setup_logger(
    name: str = "app",
    level: int = logging.INFO,
    logs_dir: str = "logs",
    *,
    backup_count: int = 7,
    use_utc: bool = False,
    at_time: dtime | None = None,
) -> logging.Logger:
    """
    Configure a logger that writes to logs/<scriptname>.log,
    rotates daily at midnight, and keeps 'backup_count' days.
    Console output happens only when level == DEBUG.
    """
    script_path = sys.argv[0]
    script_name = os.path.splitext(os.path.basename(script_path))[0]
    log_dir = os.path.join(os.path.dirname(script_path), logs_dir)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{script_name}.log")

    # Make a dedicated logger and keep it independent
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # If we re-run setup, clear handlers to avoid duplicates
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
            h.close()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # --- Timed rotating file handler (daily rollover) ---
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

    # --- Console handler (only in DEBUG mode) ---
    if level == logging.DEBUG:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(fmt)
        console.setLevel(logging.DEBUG)
        logger.addHandler(console)

    return logger
