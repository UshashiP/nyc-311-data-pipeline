"""
Transformation module for NYC 311 data pipeline.
Provides functions to clean and preprocess raw 311 data.
"""
import pandas as pd
from typing import List, Optional

def clean_311_data(
    df: pd.DataFrame,
    required_columns: Optional[List[str]] = None,
    dropna_threshold: float = 0.5,
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    import numpy as np

    if required_columns is not None:
        df = df[[col for col in required_columns if col in df.columns]]

    df = df.loc[:, df.isnull().mean() <= dropna_threshold]

    # Removed drop_duplicates() from here

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    for date_col in ["created_date", "closed_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    for lat_col, lon_col in [("latitude", "longitude"), ("incident_address_latitude", "incident_address_longitude")]:
        if lat_col in df.columns and lon_col in df.columns:
            df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
            df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
            df.loc[(df[lat_col] < 40) | (df[lat_col] > 41), lat_col] = np.nan
            df.loc[(df[lon_col] > -73) | (df[lon_col] < -75), lon_col] = np.nan

    if "borough" in df.columns:
        borough_map = {
            "BX": "BRONX", "BRONX": "BRONX",
            "BK": "BROOKLYN", "KINGS": "BROOKLYN", "BROOKLYN": "BROOKLYN",
            "MN": "MANHATTAN", "NEW YORK": "MANHATTAN", "MANHATTAN": "MANHATTAN",
            "QN": "QUEENS", "QUEENS": "QUEENS",
            "SI": "STATEN ISLAND", "RICHMOND": "STATEN ISLAND", "STATEN ISLAND": "STATEN ISLAND"
        }
        df["borough"] = df["borough"].str.upper().map(borough_map).fillna("UNKNOWN")

    if "created_date" in df.columns and "closed_date" in df.columns:
        df["response_time_hours"] = (df["closed_date"] - df["created_date"]).dt.total_seconds() / 3600

    if "borough" in df.columns:
        df["borough"] = df["borough"].fillna("UNKNOWN")
    if "complaint_type" in df.columns:
        df["complaint_type"] = df["complaint_type"].fillna("UNKNOWN")

    # Drop duplicates after all standardization, so normalized values are compared
    if drop_duplicates:
        df = df.drop_duplicates()

    return df.reset_index(drop=True)