# tests/test_ingestion.py
# Unit tests for the ingestion module
import pytest
from pathlib import Path
from src.ingestion.api_client import fetch_311_data, load_311_data_from_file
import pandas as pd

def test_fetch_311_data_small(monkeypatch):
    # Mock requests.get to avoid real API calls
    import requests
    class MockResponse:
        def __init__(self, json_data):
            self._json = json_data
        def json(self):
            return self._json
        def raise_for_status(self):
            pass
    def mock_get(*args, **kwargs):
        # Simulate two pages of 2 records each, then empty
        offset = int(kwargs['params'].get('$offset', 0))
        if offset == 0:
            return MockResponse([
                {"unique_key": "1", "complaint_type": "Noise"},
                {"unique_key": "2", "complaint_type": "Heat"},
            ])
        elif offset == 2:
            return MockResponse([
                {"unique_key": "3", "complaint_type": "Water"},
            ])
        else:
            return MockResponse([])
    monkeypatch.setattr(requests, "get", mock_get)
    df = fetch_311_data(total_limit=3, batch_size=2, verbose=False)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df['complaint_type']) == {"Noise", "Heat", "Water"}

def test_load_311_data_from_file(tmp_path):
    # Create a small CSV file
    csv_path = tmp_path / "test_311.csv"
    df = pd.DataFrame({"unique_key": [1, 2], "complaint_type": ["Noise", "Heat"]})
    df.to_csv(csv_path, index=False)
    loaded = load_311_data_from_file(csv_path)
    assert loaded.shape == (2, 2)
    assert set(loaded['complaint_type']) == {"Noise", "Heat"}
