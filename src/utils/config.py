from pathlib import Path


class Config:
    """
    Central configuration for the NYC 311 data pipeline.
    Modular, scalable, and reproducible project settings.
    """

    # Project structure
    PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"
    REPORTS_DIR = PROJECT_ROOT / "reports"
    CONFIG_DIR = PROJECT_ROOT / "config"

    # DuckDB database file
    DUCKDB_PATH = PROJECT_ROOT / "nyc311.duckdb"

    @classmethod
    def ensure_directories(cls):
        """Create all necessary directories for the pipeline."""
        for directory in [cls.DATA_DIR, cls.LOGS_DIR, 
                          cls.REPORTS_DIR, cls.CONFIG_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
