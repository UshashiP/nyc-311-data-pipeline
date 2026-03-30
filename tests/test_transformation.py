# tests/test_transformation.py
import pandas as pd
import numpy as np
from src.transformation.cleaner import clean_311_data


def test_date_parsing_and_validation():
    df = pd.DataFrame({
        "created_date": ["2024-01-01T10:00:00", "invalid", None],
        "closed_date":  ["2024-01-01T12:00:00", "2024-01-01T15:00:00", None],
    })
    cleaned = clean_311_data(df)
    assert pd.api.types.is_datetime64_any_dtype(cleaned["created_date"])
    # "invalid" and None both coerce to NaT
    assert pd.isna(cleaned.iloc[1]["created_date"])
    assert pd.isna(cleaned.iloc[2]["created_date"])


def test_coordinate_normalization():
    df = pd.DataFrame({
        "latitude":  [40.5, 39.9, 41.1, 40.7],
        "longitude": [-74.0, -74.1, -74.2, -72.5],
    })
    cleaned = clean_311_data(df)
    assert cleaned["latitude"].iloc[0] == 40.5       # valid
    assert np.isnan(cleaned["latitude"].iloc[1])     # 39.9 < 40, out of bounds
    assert np.isnan(cleaned["latitude"].iloc[2])     # 41.1 > 41, out of bounds
    assert np.isnan(cleaned["longitude"].iloc[3])    # -72.5 > -73, out of bounds


def test_borough_standardization():
    df = pd.DataFrame({
        "borough": ["BX", "bk", "Manhattan", "Queens", None]
    })
    cleaned = clean_311_data(df)
    assert list(cleaned["borough"]) == ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "UNKNOWN"]


def test_response_time_calculation():
    df = pd.DataFrame({
        "created_date": ["2024-01-01T10:00:00", "2024-01-01T10:00:00"],
        "closed_date":  ["2024-01-01T12:00:00", "2024-01-01T16:00:00"],
    })
    cleaned = clean_311_data(df)
    assert np.isclose(cleaned.iloc[0]["response_time_hours"], 2.0)
    assert np.isclose(cleaned.iloc[1]["response_time_hours"], 6.0)


def test_general_cleaning():
    df = pd.DataFrame({
        "unique_key":     [1, 1, 2],
        # Both duplicate rows have identical borough so they normalize to the same value
        "complaint_type": ["Noise", "Noise", None],
        "borough":        ["BK", "BK", "MN"],        # ← both "BK", not None vs "BK"
        "extra_col":      [None, None, None],
    })
    cleaned = clean_311_data(
        df,
        required_columns=["unique_key", "complaint_type", "borough"],
        dropna_threshold=0.5,
    )
    assert cleaned.shape[1] == 3          # extra_col dropped
    assert cleaned.shape[0] == 2          # (1, Noise, BROOKLYN) deduped to 1 row + row 2
    assert all(cleaned["complaint_type"].notna())
    assert all(cleaned["borough"].notna())


def test_dropna_threshold_drops_sparse_columns():
    """Columns with >= 50% missing values should be dropped."""
    df = pd.DataFrame({
        "unique_key":  [1, 2, 3, 4],
        "sparse_col":  [None, None, None, "x"],   # 75% missing → dropped
        "dense_col":   ["a", "b", None, "d"],      # 25% missing → kept
    })
    cleaned = clean_311_data(df, dropna_threshold=0.5)
    assert "sparse_col" not in cleaned.columns
    assert "dense_col" in cleaned.columns


def test_missing_complaint_type_filled():
    """None complaint_type should be filled with UNKNOWN."""
    df = pd.DataFrame({
        "unique_key":     [1, 2],
        "complaint_type": [None, "Noise"],
    })
    cleaned = clean_311_data(df)
    assert cleaned.iloc[0]["complaint_type"] == "UNKNOWN"
    assert cleaned.iloc[1]["complaint_type"] == "Noise"