import logging
from pathlib import Path
from .config import Config

def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger with the specified name.
    Logs to both console and a file, with formatting and level from config.
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger  # Avoid adding handlers multiple times

    log_level = logging.INFO
    log_dir = Config.LOGS_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(log_level)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    fh.setLevel(log_level)
    logger.addHandler(fh)

    logger.setLevel(log_level)
    logger.propagate = False
    return logger
