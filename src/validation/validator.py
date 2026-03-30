"""
Validation module for NYC 311 data pipeline.
Provides functions to validate cleaned 311 data before loading.
"""
import pandas as pd
from typing import List


# Columns that must be present after cleaning
REQUIRED_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "agency",
    "borough",
    "descriptor",
    "incident_zip",  
    "latitude",
    "longitude",
]

# Columns that must not exceed this null fraction
NULL_THRESHOLDS = {
    "unique_key": 0.0,       # no nulls allowed
    "created_date": 0.0,     # no nulls allowed
    "complaint_type": 0.05,  # up to 5% allowed
    "agency": 0.05,
    "borough": 0.10,
}

VALID_BOROUGHS = {"BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND", "UNKNOWN"}


class ValidationError(Exception):
    """Raised when a critical validation check fails."""
    pass


def validate_schema(df: pd.DataFrame, required_columns: List[str] = REQUIRED_COLUMNS) -> None:
    """
    Checks that all required columns are present in the DataFrame.

    Raises:
        ValidationError: If any required columns are missing.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


def validate_no_duplicates(df: pd.DataFrame, key_column: str = "unique_key") -> None:
    """
    Checks that the key column has no duplicate values.

    Raises:
        ValidationError: If duplicates are found.
    """
    if key_column not in df.columns:
        raise ValidationError(f"Key column '{key_column}' not found in DataFrame.")
    n_dupes = df[key_column].duplicated().sum()
    if n_dupes > 0:
        raise ValidationError(f"Found {n_dupes} duplicate values in '{key_column}'.")


def validate_null_rates(
    df: pd.DataFrame,
    thresholds: dict = NULL_THRESHOLDS,
) -> None:
    """
    Checks that null rates for critical columns are within acceptable thresholds.

    Raises:
        ValidationError: If any column exceeds its null threshold.
    """
    violations = []
    for col, threshold in thresholds.items():
        if col not in df.columns:
            continue
        null_rate = df[col].isnull().mean()
        if null_rate > threshold:
            violations.append(f"'{col}': {null_rate:.1%} nulls (max {threshold:.1%})")
    if violations:
        raise ValidationError(f"Null rate violations: {violations}")


def validate_date_logic(df: pd.DataFrame) -> None:
    """
    Checks that closed_date is never earlier than created_date.

    Raises:
        ValidationError: If any rows have closed_date before created_date.
    """
    if "created_date" not in df.columns or "closed_date" not in df.columns:
        return
    invalid = df.dropna(subset=["created_date", "closed_date"])
    invalid = invalid[invalid["closed_date"] < invalid["created_date"]]
    if len(invalid) > 0:
        raise ValidationError(
            f"Found {len(invalid)} rows where closed_date is before created_date."
        )


def validate_borough_values(df: pd.DataFrame) -> None:
    """
    Checks that borough column contains only known values.

    Raises:
        ValidationError: If unexpected borough values are found.
    """
    if "borough" not in df.columns:
        return
    unexpected = set(df["borough"].dropna().unique()) - VALID_BOROUGHS
    if unexpected:
        raise ValidationError(f"Unexpected borough values: {unexpected}")


def validate_row_count(df: pd.DataFrame, min_rows: int = 1) -> None:
    """
    Checks that the DataFrame has at least min_rows rows.

    Raises:
        ValidationError: If the DataFrame has too few rows.
    """
    if len(df) < min_rows:
        raise ValidationError(f"DataFrame has {len(df)} rows, expected at least {min_rows}.")


def run_all_validations(df: pd.DataFrame) -> None:
    """
    Runs all validation checks on the DataFrame.
    Raises ValidationError on the first failed check.

    Args:
        df: Cleaned DataFrame to validate.

    Raises:
        ValidationError: If any check fails.
    """
    validate_row_count(df)
    validate_schema(df)
    validate_no_duplicates(df)
    validate_null_rates(df)
    validate_date_logic(df)
    validate_borough_values(df)