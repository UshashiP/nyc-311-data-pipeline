from pathlib import Path
import pandas as pd


RAW_BASE = Path("data/raw/nyc_311")
CLEAN_BASE = Path("data/clean/nyc_311")


def read_latest_raw() -> pd.DataFrame:
    """Read the most recent raw ingestion folder."""
    ingestion_dirs = sorted(RAW_BASE.glob("ingested_at=*"))
    if not ingestion_dirs:
        raise FileNotFoundError("No raw ingestion folders found")

    latest_dir = ingestion_dirs[-1]
    parquet_files = list(latest_dir.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files in {latest_dir}")

    return pd.read_parquet(parquet_files[0])


def clean_311(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize NYC 311 data."""
    df = df.rename(columns=str.lower)

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce", utc=True)
    df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce", utc=True)

    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")

    df["borough"] = (
        df["borough"]
        .str.strip()
        .str.title()
        .replace({"N/a": None, "": None})
    )

    df = df.drop_duplicates(subset="unique_key")

    df["response_time_minutes"] = (
        (df["closed_date"] - df["created_date"])
        .dt.total_seconds() / 60
    )

    return df


def write_clean_parquet(df: pd.DataFrame):
    """Write clean parquet partitioned by year/month."""
    df["year"] = df["created_date"].dt.year
    df["month"] = df["created_date"].dt.month

    CLEAN_BASE.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        CLEAN_BASE,
        partition_cols=["year", "month"],
        index=False,
    )


def transform_data():
    """Transform and clean raw data."""
    print("Reading raw data")
    raw_df = read_latest_raw()
    print(f"Raw rows: {len(raw_df):,}")

    print("Cleaning data")
    clean_df = clean_311(raw_df)
    print(f"Clean rows: {len(clean_df):,}")

    print("Writing clean parquet")
    write_clean_parquet(clean_df)

    print("Step 2 complete")