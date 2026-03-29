# tests/test_logger.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.utils.logger import get_logger

def test_get_logger_creates_logger():
    logger = get_logger("test_logger")
    assert logger.name == "test_logger"
    logger.info("Logger test message")
    # No exception means success; check log file manually if needed
