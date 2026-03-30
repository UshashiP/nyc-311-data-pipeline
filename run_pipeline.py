"""
NYC 311 Data Pipeline - Main Entry Point
Orchestrates ingestion, transformation, validation, loading, and analytics.
"""
import sys
from pathlib import Path
from datetime import datetime

from src.utils.config import load_config, ensure_directories
from src.utils.logger import get_logger
from src.ingestion.api_client import fetch_311_data
from src.transformation.cleaner import clean_311_data
from src.validation.validator import run_all_validations, ValidationError
from src.loading.loader import save_to_duckdb, save_to_parquet
from src.analytics.build_analytics import build_star_schema
from src.analytics.reports import generate_reports


def run_pipeline(config_path: Path = None) -> None:
    """
    Runs the full NYC 311 data pipeline end-to-end.

    Steps:
        1. Load config and set up directories
        2. Ingest raw data from NYC Open Data API
        3. Clean and transform data
        4. Validate cleaned data
        5. Load to DuckDB and Parquet
        6. Build star schema
        7. Generate analytics reports

    Args:
        config_path: Path to YAML config file. Defaults to config/pipeline_config.yaml.
    """
    # ── Step 1: Config + setup ─────────────────────────────────────────────
    config = load_config(config_path)
    ensure_directories(config)

    log_dir = Path(config["paths"]["logs"])
    logger = get_logger("pipeline", log_dir=log_dir)
    logger.info("=" * 60)
    logger.info("NYC 311 Data Pipeline started")
    logger.info(f"Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

    try:
        # ── Step 2: Ingestion ──────────────────────────────────────────────
        logger.info("Step 1/6 — Ingesting data from NYC Open Data API")
        raw_df = fetch_311_data(
            total_limit=config["api"]["total_limit"],
            app_token=config["api"].get("app_token"),
            batch_size=config["api"]["batch_size"],
            start_date=config["api"].get("start_date"),
            end_date=config["api"].get("end_date"),
            save_path=Path(config["paths"]["raw_data"]) / "nyc_311_raw.parquet",
            verbose=True,
        )
        logger.info(f"Ingested {len(raw_df):,} raw records")

        if raw_df.empty:
            logger.error("No data fetched — aborting pipeline")
            sys.exit(1)

        # ── Step 3: Transformation ─────────────────────────────────────────
        logger.info("Step 2/6 — Cleaning and transforming data")
        cleaned_df = clean_311_data(
            raw_df,
            required_columns=config["cleaning"]["required_columns"],
            drop_duplicates=config["cleaning"]["drop_duplicates"],
        )
        logger.info(f"Cleaned data: {len(cleaned_df):,} rows, {cleaned_df.shape[1]} columns")

        # ── Step 4: Validation ─────────────────────────────────────────────
        logger.info("Step 3/6 — Validating cleaned data")
        try:
            run_all_validations(cleaned_df)
            logger.info("Validation passed")
        except ValidationError as e:
            logger.error(f"Validation failed: {e}")
            sys.exit(1)

        # ── Step 5: Loading ────────────────────────────────────────────────
        logger.info("Step 4/6 — Loading data to DuckDB and Parquet")
        save_to_parquet(cleaned_df, Path(config["output"]["parquet_path"]))
        save_to_duckdb(
            cleaned_df,
            db_path=Path(config["output"]["duckdb_path"]),
            table_name=config["output"]["table_name"],
        )
        logger.info(f"Saved to {config['output']['parquet_path']}")
        logger.info(f"Saved to {config['output']['duckdb_path']}")

        # ── Step 6: Star schema ────────────────────────────────────────────
        logger.info("Step 5/6 — Building star schema")
        counts = build_star_schema(
            clean_dir=Path(config["paths"]["clean_data"]),
            analytics_dir=Path(config["paths"]["analytics"]),
        )
        for table, count in counts.items():
            logger.info(f"  {table}: {count:,} rows")

        # ── Step 7: Reports ────────────────────────────────────────────────
        logger.info("Step 6/6 — Generating analytics reports")
        results = generate_reports(
            analytics_dir=Path(config["paths"]["analytics"]),
            reports_dir=Path(config["paths"]["reports"]),
        )
        logger.info(f"Generated {len(results)} reports → {config['paths']['reports']}")

        logger.info("Pipeline completed successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()