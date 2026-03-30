# tests/test_validation.py
# Unit tests for the validation module
import pandas as pd
import pytest
from src.validation.validator import (
    validate_schema,
    validate_no_duplicates,
    validate_null_rates,
    validate_date_logic,
    validate_borough_values,
    validate_row_count,
    run_all_validations,
    ValidationError,
)


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture
def valid_df():
    return pd.DataFrame({
        "unique_key":     ["1", "2", "3"],
        "created_date":   pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "closed_date":    pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
        "complaint_type": ["Noise", "Graffiti", "HEAT/HOT WATER"],
        "agency":         ["NYPD", "DSNY", "HPD"],
        "borough":        ["MANHATTAN", "BROOKLYN", "BRONX"],
    })


# ── validate_schema ────────────────────────────────────────────────────────

def test_schema_passes_with_all_columns(valid_df):
    validate_schema(valid_df)  # should not raise


def test_schema_raises_on_missing_column(valid_df):
    with pytest.raises(ValidationError, match="Missing required columns"):
        validate_schema(valid_df.drop(columns=["unique_key"]))


# ── validate_no_duplicates ─────────────────────────────────────────────────

def test_no_duplicates_passes(valid_df):
    validate_no_duplicates(valid_df)  # should not raise


def test_no_duplicates_raises_on_dupes(valid_df):
    with pytest.raises(ValidationError, match="not found"):
        validate_no_duplicates(valid_df, key_column="nonexistent_col")


# ── validate_null_rates ────────────────────────────────────────────────────

def test_null_rates_passes(valid_df):
    validate_null_rates(valid_df)  # should not raise


def test_null_rates_raises_on_violation(valid_df):
    valid_df["unique_key"] = None  # 100% nulls, threshold is 0%
    with pytest.raises(ValidationError, match="Null rate violations"):
        validate_null_rates(valid_df)


# ── validate_date_logic ────────────────────────────────────────────────────

def test_date_logic_passes(valid_df):
    validate_date_logic(valid_df)  # should not raise


def test_date_logic_raises_when_closed_before_created(valid_df):
    valid_df.loc[0, "closed_date"] = pd.Timestamp("2023-12-31")  # before created
    with pytest.raises(ValidationError, match="closed_date is before created_date"):
        validate_date_logic(valid_df)


def test_date_logic_skips_nulls(valid_df):
    valid_df.loc[0, "closed_date"] = None  # null closed_date should be ignored
    validate_date_logic(valid_df)  # should not raise


# ── validate_borough_values ────────────────────────────────────────────────

def test_borough_passes(valid_df):
    validate_borough_values(valid_df)  # should not raise


def test_borough_raises_on_unexpected_value(valid_df):
    valid_df.loc[0, "borough"] = "NARNIA"
    with pytest.raises(ValidationError, match="Unexpected borough values"):
        validate_borough_values(valid_df)


# ── validate_row_count ─────────────────────────────────────────────────────

def test_row_count_passes(valid_df):
    validate_row_count(valid_df, min_rows=1)  # should not raise


def test_row_count_raises_on_empty():
    with pytest.raises(ValidationError, match="0 rows"):
        validate_row_count(pd.DataFrame(), min_rows=1)


# ── run_all_validations ────────────────────────────────────────────────────

def test_run_all_passes(valid_df):
    run_all_validations(valid_df)  # should not raise


def test_run_all_raises_on_invalid(valid_df):
    valid_df.loc[0, "borough"] = "INVALID"
    with pytest.raises(ValidationError):
        run_all_validations(valid_df)