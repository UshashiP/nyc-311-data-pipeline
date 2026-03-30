# tests/test_config.py
import pytest
from pathlib import Path
from src.utils.config import load_config, ensure_directories


def test_load_config_defaults(tmp_path):
    """Should load config from default path successfully."""
    config = load_config()
    assert "api" in config
    assert "paths" in config
    assert "cleaning" in config
    assert "output" in config


def test_load_config_raises_on_missing_file(tmp_path):
    """Should raise FileNotFoundError if config file doesn't exist."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config(config_path=tmp_path / "nonexistent.yaml")


def test_ensure_directories_creates_paths(tmp_path):
    """Should create all directories defined in config."""
    config = {
        "paths": {
            "raw_data": tmp_path / "data/raw",
            "logs": tmp_path / "logs",
        },
        "output": {
            "duckdb_path": tmp_path / "data/db/nyc_311.duckdb",
            "parquet_path": tmp_path / "data/clean/nyc_311.parquet",
        }
    }
    ensure_directories(config)
    assert (tmp_path / "data/raw").exists()
    assert (tmp_path / "logs").exists()
    assert (tmp_path / "data/db").exists()