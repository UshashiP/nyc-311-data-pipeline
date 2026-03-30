import os
import yaml
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()


def load_config(config_path: Path = None) -> dict:
    """
    Loads pipeline configuration from YAML file.
    Overrides app_token with SOCRATA_APP_TOKEN env variable if set.

    Args:
        config_path: Path to YAML config file. Defaults to config/pipeline_config.yaml.

    Returns:
        Dict with pipeline configuration.

    Raises:
        FileNotFoundError: If config file does not exist.
    """
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "pipeline_config.yaml"

    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Override app_token from environment variable if set
    env_token = os.getenv("SOCRATA_APP_TOKEN")
    if env_token:
        config["api"]["app_token"] = env_token

    # Resolve all paths relative to project root
    for key, val in config.get("paths", {}).items():
        config["paths"][key] = PROJECT_ROOT / val

    config["output"]["duckdb_path"] = PROJECT_ROOT / config["output"]["duckdb_path"]
    config["output"]["parquet_path"] = PROJECT_ROOT / config["output"]["parquet_path"]

    return config


def ensure_directories(config: dict) -> None:
    """Creates all directories defined in config paths."""
    for path in config.get("paths", {}).values():
        Path(path).mkdir(parents=True, exist_ok=True)
    Path(config["output"]["duckdb_path"]).parent.mkdir(parents=True, exist_ok=True)