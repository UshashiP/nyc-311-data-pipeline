# tests/test_transformation.py
# Unit tests for the transformation/cleaner module
import pandas as pd
import numpy as np
from src.transformation.cleaner import clean_311_data

def test_date_parsing_and_validation():
    df = pd.DataFrame({
        "created_date": ["2024-01-01T10:00:00", "invalid", None],
        "closed_date": ["2024-01-01T12:00:00", "2024-01-01T15:00:00", None],
    })
    cleaned = clean_311_data(df)
    assert pd.api.types.is_datetime64_any_dtype(cleaned["created_date"])
    assert pd.isna(cleaned.loc[1, "created_date"])
    assert pd.isna(cleaned.loc[2, "created_date"])


def test_coordinate_normalization():
    df = pd.DataFrame({
        "latitude": [40.5, 39.9, 41.1, None],
        "longitude": [-74.0, -72.5, -75.1, None],
    })
    cleaned = clean_311_data(df)
    assert np.isnan(cleaned.loc[1, "latitude"])  # Out of bounds
    assert np.isnan(cleaned.loc[2, "latitude"])  # Out of bounds
    assert np.isnan(cleaned.loc[1, "longitude"])  # Out of bounds
    assert np.isnan(cleaned.loc[2, "longitude"])  # Out of bounds


def test_borough_standardization():
    df = pd.DataFrame({"borough": ["BX", "bk", "Manhattan", "unknown", None]})
    cleaned = clean_311_data(df)
    assert list(cleaned["borough"]) == ["BRONX", "BROOKLYN", "MANHATTAN", "UNKNOWN", "UNKNOWN"]


def test_response_time_calculation():
    df = pd.DataFrame({
        "created_date": ["2024-01-01T10:00:00", "2024-01-01T10:00:00"],
        "closed_date": ["2024-01-01T12:00:00", "2024-01-01T16:00:00"],
    })
    cleaned = clean_311_data(df)
    assert np.isclose(cleaned.loc[0, "response_time_hours"], 2.0)
    assert np.isclose(cleaned.loc[1, "response_time_hours"], 6.0)


def test_general_cleaning():
    df = pd.DataFrame({
        "unique_key": [1, 1, 2],
        "complaint_type": [None, "Noise", None],
        "borough": [None, "BK", "MN"],
        "extra_col": [None, None, None],
    })
    cleaned = clean_311_data(df, required_columns=["unique_key", "complaint_type", "borough"], dropna_threshold=0.5)
    assert cleaned.shape[1] == 3  # Only required columns
    assert cleaned.shape[0] == 2  # Duplicates dropped
    assert all(cleaned["complaint_type"].notna())
    assert all(cleaned["borough"].notna())
