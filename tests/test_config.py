# tests/test_config.py
# Unit tests for the config utility

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.utils.config import Config


def test_project_root_exists():
    assert Config.PROJECT_ROOT.exists()


def test_data_dir_path():
    assert str(Config.DATA_DIR).endswith("data")


def test_duckdb_path():
    assert str(Config.DUCKDB_PATH).endswith("nyc311.duckdb")
