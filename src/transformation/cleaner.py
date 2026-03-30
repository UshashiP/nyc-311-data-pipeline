"""
Transformation module for NYC 311 data pipeline.
Provides functions to clean and preprocess raw 311 data.
"""
import numpy as np
import pandas as pd
from typing import List, Optional


# Categorical columns that should be filled with UNKNOWN when null
CATEGORICAL_UNKNOWN_COLS = {
    "borough", "complaint_type", "agency", "descriptor",
    "status", "city", "address_type", "agency_name",
}


def _fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputes missing values based on column type and distribution.

    Strategy:
        - unique_key: drop rows (unfixable)
        - Date columns: leave as NaT (null is meaningful)
        - Numeric columns: fill with median (robust to skew)
        - Known categorical columns: fill with "UNKNOWN"
        - Other string columns: fill with mode
    """
    # Drop rows where unique_key is null — cannot be recovered
    if "unique_key" in df.columns:
        before = len(df)
        df = df.dropna(subset=["unique_key"])
        dropped = before - len(df)
        if dropped > 0:
            print(f"Dropped {dropped} rows with null unique_key")

    for col in df.columns:
        if df[col].isnull().sum() == 0:
            continue

        # Date columns — NaT is meaningful, do not fill
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        # Numeric columns — fill with median (robust to outliers and skew)
        elif pd.api.types.is_numeric_dtype(df[col]):
            median = df[col].median()
            df[col] = df[col].fillna(median)

        # Known categorical columns — fill with UNKNOWN
        elif col in CATEGORICAL_UNKNOWN_COLS:
            df[col] = df[col].fillna("UNKNOWN")

        # All other string/object columns — fill with mode
        else:
            mode = df[col].mode()
            if not mode.empty:
                df[col] = df[col].fillna(mode[0])

    return df


def clean_311_data(
    df: pd.DataFrame,
    required_columns: Optional[List[str]] = None,
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Cleans and preprocesses raw NYC 311 data.

    Steps:
        1. Keep only required columns
        2. Drop columns that are entirely empty (100% null)
        3. Standardize column names
        4. Parse date columns
        5. Normalize coordinates
        6. Standardize borough values
        7. Calculate response time
        8. Fill missing values intelligently
        9. Drop duplicates

    Args:
        df: Raw 311 data as a DataFrame.
        required_columns: List of columns to keep (None to keep all).
        drop_duplicates: Whether to drop duplicate rows.

    Returns:
        Cleaned DataFrame.
    """
    # Step 1 — Keep only required columns
    if required_columns is not None:
        df = df[[col for col in required_columns if col in df.columns]]

    # Step 2 — Drop columns that are entirely empty
    df = df.loc[:, df.isnull().mean() < 1.0]

    # Step 3 — Standardize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Step 4 — Parse date columns
    for date_col in ["created_date", "closed_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Step 5 — Normalize coordinates
    for lat_col, lon_col in [
        ("latitude", "longitude"),
        ("incident_address_latitude", "incident_address_longitude"),
    ]:
        if lat_col in df.columns and lon_col in df.columns:
            df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
            df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
            df.loc[(df[lat_col] < 40) | (df[lat_col] > 41), lat_col] = np.nan
            df.loc[(df[lon_col] > -73) | (df[lon_col] < -75), lon_col] = np.nan

    # Step 6 — Standardize borough values
    if "borough" in df.columns:
        borough_map = {
            "BX": "BRONX", "BRONX": "BRONX",
            "BK": "BROOKLYN", "KINGS": "BROOKLYN", "BROOKLYN": "BROOKLYN",
            "MN": "MANHATTAN", "NEW YORK": "MANHATTAN", "MANHATTAN": "MANHATTAN",
            "QN": "QUEENS", "QUEENS": "QUEENS",
            "SI": "STATEN ISLAND", "RICHMOND": "STATEN ISLAND",
            "STATEN ISLAND": "STATEN ISLAND",
        }
        df["borough"] = df["borough"].str.upper().map(borough_map).fillna("UNKNOWN")

    if "created_date" in df.columns and "closed_date" in df.columns:
        mask = (
        df["closed_date"].notna() &
        df["created_date"].notna() &
        (df["closed_date"] < df["created_date"])
        )
        df.loc[mask, "closed_date"] = pd.NaT

    # Step 7 — Calculate response time
    if "created_date" in df.columns and "closed_date" in df.columns:
        df["response_time_hours"] = (
            (df["closed_date"] - df["created_date"]).dt.total_seconds() / 3600
        )

    # Step 8 — Imputing missing values
    df = _fill_missing_values(df)

    # Step 9 — Drop duplicates after all standardization
    if drop_duplicates:
        df = df.drop_duplicates()

    return df.reset_index(drop=True)