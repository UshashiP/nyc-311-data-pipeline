"""
Loading module for NYC 311 data pipeline.
Provides functions to save cleaned data to DuckDB and Parquet files.
"""
import pandas as pd
from pathlib import Path
import duckdb


def save_to_duckdb(
    df: pd.DataFrame,
    db_path: Path,
    table_name: str = "nyc_311_cleaned",
) -> None:
    """
    Saves a DataFrame to a DuckDB database table.
    Overwrites the table if it already exists.

    Args:
        df: DataFrame to save.
        db_path: Path to the DuckDB database file.
        table_name: Name of the table to write to.

    Raises:
        ValueError: If table_name is not a valid identifier.
    """
    if not table_name.isidentifier():
        raise ValueError(f"Invalid table name: '{table_name}'")

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")


def save_to_parquet(df: pd.DataFrame, file_path: Path) -> None:
    """
    Saves a DataFrame to a Parquet file.

    Args:
        df: DataFrame to save.
        file_path: Path to the Parquet file.
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path, index=False)