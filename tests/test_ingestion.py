# tests/test_ingestion.py
import pandas as pd
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.ingestion.api_client import fetch_311_data, load_311_data_from_file


def make_batch(n: int, start: int = 0) -> list[dict]:
    return [{"unique_key": str(start + i), "complaint_type": "Noise"} for i in range(n)]


def mock_response(data: list) -> MagicMock:
    mock = MagicMock()
    mock.json.return_value = data
    mock.raise_for_status.return_value = None
    return mock


@patch("src.ingestion.api_client.requests.Session")
def test_fetch_paginates_and_respects_limit(mock_session_cls):
    """Should paginate and stop at total_limit."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_session.get.side_effect = [
        mock_response(make_batch(5, start=0)),
        mock_response(make_batch(5, start=5)),
        mock_response([]),
    ]
    df = fetch_311_data(total_limit=7, batch_size=5, verbose=False)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 7


@patch("src.ingestion.api_client.requests.Session")
def test_fetch_saves_file(mock_session_cls):
    """Should save output to disk."""
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_session.get.return_value = mock_response(make_batch(3))
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "output.parquet"
        fetch_311_data(total_limit=3, save_path=path, verbose=False)
        assert path.exists()


@patch("src.ingestion.api_client.requests.Session")
def test_fetch_raises_on_http_error(mock_session_cls):
    """Should propagate HTTP errors."""
    import requests
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_obj = MagicMock()
    mock_obj.raise_for_status.side_effect = requests.HTTPError("500")
    mock_session.get.return_value = mock_obj
    with pytest.raises(requests.HTTPError):
        fetch_311_data(total_limit=5, verbose=False)


def test_load_from_file():
    """Should load Parquet and CSV files correctly."""
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    with tempfile.TemporaryDirectory() as tmpdir:
        for ext, writer in [(".parquet", df.to_parquet), (".csv", df.to_csv)]:
            path = Path(tmpdir) / f"data{ext}"
            writer(path, index=False)
            assert load_311_data_from_file(path).equals(df)


def test_load_file_not_found():
    """Should raise FileNotFoundError for missing files."""
    with pytest.raises(FileNotFoundError):
        load_311_data_from_file(Path("/nonexistent/data.parquet"))