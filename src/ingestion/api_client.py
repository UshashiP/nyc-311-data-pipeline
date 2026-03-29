"""
Ingestion module for NYC 311 data pipeline.
Provides functions to fetch data from the NYC Open Data API or load from a local file.
"""
import requests
import pandas as pd
from pathlib import Path
from typing import Optional

NYC_311_API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


def fetch_311_data(
    total_limit: Optional[int] = None,
    app_token: Optional[str] = None,
    save_path: Optional[Path] = None,
    batch_size: int = 1000,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Fetches NYC 311 data from the Open Data API, handling pagination to retrieve all records.
    Args:
        total_limit: Total number of records to fetch (None for all available).
        app_token: Optional Socrata app token for higher rate limits.
        save_path: Optional path to save the raw data as a CSV.
        batch_size: Number of records per API request (max 1000).
        verbose: Print progress if True.
    Returns:
        DataFrame with 311 data.
    """
    headers = {"X-App-Token": app_token} if app_token else {}
    all_data = []
    offset = 0
    fetched = 0
    while True:
        limit = batch_size
        if total_limit is not None:
            limit = min(batch_size, total_limit - fetched)
            if limit <= 0:
                break
        params = {"$limit": limit, "$offset": offset}
        response = requests.get(NYC_311_API_URL, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        fetched += len(data)
        offset += len(data)
        if verbose:
            print(f"Fetched {fetched} records..")
        if total_limit is not None and fetched >= total_limit:
            break
        if len(data) < batch_size:
            break  # No more data available
    df = pd.DataFrame(all_data)
    if save_path:
        df.to_csv(save_path, index=False)
    return df

def load_311_data_from_file(file_path: Path) -> pd.DataFrame:
    """
    Loads NYC 311 data from a local CSV file.
    Args:
        file_path: Path to the CSV file.
    Returns:
        DataFrame with 311 data.
    """
    return pd.read_csv(file_path)
