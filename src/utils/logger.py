import logging
from pathlib import Path


def get_logger(name: str, log_dir: Path = Path("logs")) -> logging.Logger:
    """
    Returns a logger with the specified name.
    Logs to both console and a file.

    Args:
        name: Logger name (typically __name__ of the calling module).
        log_dir: Directory to write log file. Defaults to logs/.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.INFO)

    fh = logging.FileHandler(log_dir / "pipeline.log")
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger