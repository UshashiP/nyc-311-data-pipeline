# tests/test_loader.py
# Unit tests for the loading module
import pandas as pd
import pytest
from pathlib import Path
import duckdb
import tempfile
from src.loading.loader import save_to_duckdb, save_to_parquet


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


@pytest.fixture
def empty_df():
    return pd.DataFrame({"a": pd.Series([], dtype="int64"), "b": pd.Series([], dtype="str")})


# ── DuckDB tests ───────────────────────────────────────────────────────────

def test_save_to_duckdb_basic(sample_df):
    """Saved data should match the original DataFrame."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        save_to_duckdb(sample_df, db_path, table_name="test_table")
        with duckdb.connect(str(db_path)) as con:
            result = con.execute("SELECT * FROM test_table ORDER BY a").fetchdf()
        assert result.equals(sample_df)


def test_save_to_duckdb_overwrites(sample_df):
    """Saving twice should not duplicate rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        save_to_duckdb(sample_df, db_path, table_name="test_table")
        save_to_duckdb(sample_df, db_path, table_name="test_table")  # second write
        with duckdb.connect(str(db_path)) as con:
            count = con.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
        assert count == len(sample_df)  # no duplicates


def test_save_to_duckdb_overwrites_schema(sample_df):
    """Saving a DataFrame with a different schema should replace the table cleanly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        save_to_duckdb(sample_df, db_path, table_name="test_table")
        new_df = pd.DataFrame({"x": [10, 20], "y": [30, 40], "z": [50, 60]})
        save_to_duckdb(new_df, db_path, table_name="test_table")
        with duckdb.connect(str(db_path)) as con:
            result = con.execute("SELECT * FROM test_table ORDER BY x").fetchdf()
        assert list(result.columns) == ["x", "y", "z"]
        assert len(result) == 2


def test_save_to_duckdb_empty_dataframe(empty_df):
    """Saving an empty DataFrame should create the table with no rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        save_to_duckdb(empty_df, db_path, table_name="test_table")
        with duckdb.connect(str(db_path)) as con:
            count = con.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
        assert count == 0


def test_save_to_duckdb_invalid_table_name(sample_df):
    """Invalid table names should raise a ValueError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        with pytest.raises(ValueError, match="Invalid table name"):
            save_to_duckdb(sample_df, db_path, table_name="bad-name!")


def test_save_to_duckdb_creates_parent_dirs(sample_df):
    """Parent directories should be created if they don't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "nested" / "dir" / "test.duckdb"
        save_to_duckdb(sample_df, db_path)
        assert db_path.exists()


# ── Parquet tests ──────────────────────────────────────────────────────────

def test_save_to_parquet_basic(sample_df):
    """Saved parquet should match the original DataFrame."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pq_path = Path(tmpdir) / "test.parquet"
        save_to_parquet(sample_df, pq_path)
        loaded = pd.read_parquet(pq_path)
        assert loaded.equals(sample_df)


def test_save_to_parquet_overwrites(sample_df):
    """Saving twice should overwrite, not append."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pq_path = Path(tmpdir) / "test.parquet"
        save_to_parquet(sample_df, pq_path)
        save_to_parquet(sample_df, pq_path)
        loaded = pd.read_parquet(pq_path)
        assert len(loaded) == len(sample_df)  # no duplicates


def test_save_to_parquet_empty_dataframe(empty_df):
    """Saving an empty DataFrame should produce a readable parquet with no rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pq_path = Path(tmpdir) / "test.parquet"
        save_to_parquet(empty_df, pq_path)
        loaded = pd.read_parquet(pq_path)
        assert len(loaded) == 0
        assert list(loaded.columns) == ["a", "b"]


def test_save_to_parquet_creates_parent_dirs(sample_df):
    """Parent directories should be created if they don't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pq_path = Path(tmpdir) / "nested" / "dir" / "test.parquet"
        save_to_parquet(sample_df, pq_path)
        assert pq_path.exists()