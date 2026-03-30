"""
Ingestion module for NYC 311 data pipeline.
Provides functions to fetch data from the NYC Open Data API or load from a local file.
"""
import requests
import pandas as pd
from pathlib import Path
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NYC_311_API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


def _create_session(app_token: Optional[str] = None) -> requests.Session:
    """
    Creates a requests session with retry logic and optional app token.
    Retries on 429 (rate limit), 500, 502, 503, 504.
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    if app_token:
        session.headers.update({"X-App-Token": app_token})
    return session


def fetch_311_data(
    total_limit: Optional[int] = None,
    app_token: Optional[str] = None,
    save_path: Optional[Path] = None,
    batch_size: int = 1000,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Fetches NYC 311 data from the Open Data API with pagination and retry logic.

    Args:
        total_limit: Total number of records to fetch (None for all available).
        app_token: Optional Socrata app token for higher rate limits.
        save_path: Optional path to save raw data (CSV or Parquet based on extension).
        batch_size: Number of records per API request (max 1000).
        start_date: Optional start date filter (e.g. "2025-01-01").
        end_date: Optional end date filter (e.g. "2025-03-30").
        verbose: Print progress if True.

    Returns:
        DataFrame with 311 data.

    Raises:
        requests.HTTPError: If the API returns a non-2xx response after retries.
        ValueError: If batch_size is not between 1 and 1000.
    """
    if not 1 <= batch_size <= 1000:
        raise ValueError(f"batch_size must be between 1 and 1000, got {batch_size}")

    # Build optional date filter
    where_clauses = []
    if start_date:
        where_clauses.append(f"created_date >= '{start_date}T00:00:00'")
    if end_date:
        where_clauses.append(f"created_date <= '{end_date}T23:59:59'")
    where_clause = " AND ".join(where_clauses) if where_clauses else None

    session = _create_session(app_token)
    all_data = []
    offset = 0
    fetched = 0

    while True:
        limit = batch_size
        if total_limit is not None:
            limit = min(batch_size, total_limit - fetched)
            if limit <= 0:
                break

        params = {"$limit": limit, "$offset": offset, "$order": "created_date DESC"}
        if where_clause:
            params["$where"] = where_clause

        response = session.get(NYC_311_API_URL, params=params, timeout=60)
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        all_data.extend(batch)
        fetched += len(batch)
        offset += len(batch)

        if verbose:
            print(f"Fetched {min(fetched, total_limit or fetched)} records...")

        if total_limit is not None and fetched >= total_limit:
            all_data = all_data[:total_limit]
            break
        if len(batch) < batch_size:
            break

    df = pd.DataFrame(all_data)

    if save_path:
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        if save_path.suffix == ".parquet":
            df.to_parquet(save_path, index=False)
        elif save_path.suffix == ".csv":
            df.to_csv(save_path, index=False)
        else:
            raise ValueError(f"Unsupported file format: '{save_path.suffix}'. Use .parquet or .csv")

    return df


def load_311_data_from_file(file_path: Path) -> pd.DataFrame:
    """
    Loads NYC 311 data from a local CSV or Parquet file.

    Args:
        file_path: Path to the data file (.csv or .parquet).

    Returns:
        DataFrame with 311 data.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file format is not supported.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.suffix == ".parquet":
        return pd.read_parquet(file_path)
    elif file_path.suffix == ".csv":
        return pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: '{file_path.suffix}'. Use .parquet or .csv")