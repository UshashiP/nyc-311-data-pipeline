# tests/test_analytics.py
import pandas as pd
import pytest
from pathlib import Path
from src.analytics.build_analytics import build_star_schema
from src.analytics.reports import generate_reports


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture
def clean_dir(tmp_path):
    df = pd.DataFrame({
        "unique_key":          ["1", "2", "3"],
        "created_date":        pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "closed_date":         pd.to_datetime(["2024-01-02", "2024-01-04", "2024-01-05"]),
        "response_time_hours": [24.0, 48.0, 48.0],
        "agency":              ["NYPD", "DSNY", "HPD"],
        "complaint_type":      ["Noise", "Graffiti", "HEAT/HOT WATER"],
        "descriptor":          ["Loud Music", "Wall", "No Heat"],
        "borough":             ["MANHATTAN", "BROOKLYN", "BRONX"],
        "incident_zip":        ["10001", "11201", "10451"],
        "latitude":            [40.75, 40.69, 40.84],
        "longitude":           [-73.99, -73.99, -73.93],
    })
    p = tmp_path / "clean"
    p.mkdir()
    df.to_parquet(p / "data.parquet", index=False)
    return p


@pytest.fixture
def analytics_dir(tmp_path, clean_dir):
    p = tmp_path / "analytics"
    build_star_schema(clean_dir, p)
    return p


# ── star_schema ────────────────────────────────────────────────────────────

def test_star_schema_creates_all_files(clean_dir, tmp_path):
    """All 5 Parquet files should be created with correct row counts."""
    p = tmp_path / "analytics"
    counts = build_star_schema(clean_dir, p)
    expected_files = ["dim_agency", "dim_complaint", "dim_location", "dim_date", "fact_311_requests"]
    for f in expected_files:
        assert (p / f"{f}.parquet").exists()
    assert counts["fact_311_requests"] == 3
    assert counts["dim_agency"] == 3  # NYPD, DSNY, HPD — all distinct


def test_star_schema_raises_on_missing_clean_dir(tmp_path):
    with pytest.raises(FileNotFoundError, match="Clean data directory not found"):
        build_star_schema(tmp_path / "nonexistent", tmp_path / "analytics")


# ── reports ────────────────────────────────────────────────────────────────

def test_generate_reports_creates_csvs_and_returns_dataframes(analytics_dir, tmp_path):
    """All 6 CSVs should be created and results returned as DataFrames."""
    p = tmp_path / "reports"
    results = generate_reports(analytics_dir, p)
    expected = [
        "avg_response_time_by_borough",
        "top_20_complaint_types",
        "top_agencies_by_volume",
        "monthly_volume_trend",
        "slowest_complaints_by_avg_time",
        "data_quality_null_rates",
    ]
    for name in expected:
        assert (p / f"{name}.csv").exists()
        assert isinstance(results[name], pd.DataFrame)


def test_generate_reports_raises_on_missing_analytics(tmp_path):
    with pytest.raises(FileNotFoundError, match="Missing analytics files"):
        generate_reports(tmp_path / "empty", tmp_path / "reports")