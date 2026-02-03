import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


NYC_DOMAIN = "https://data.cityofnewyork.us"
RESOURCE_ID = "erm2-nwe9"
ENDPOINT = f"{NYC_DOMAIN}/resource/{RESOURCE_ID}.json"


def fetch_page(
    session: requests.Session,
    limit: int,
    offset: int,
    where: Optional[str],
    app_token: Optional[str],
) -> list[dict]:
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "created_date ASC, unique_key ASC",
    }
    if where:
        params["$where"] = where

    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    resp = session.get(ENDPOINT, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def write_parquet(df: pd.DataFrame, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "part-00000.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def ingest_raw_data(out="data/raw/nyc_311", limit=50000, max_pages=1, where=None, app_token=None):
    """Ingest raw data from NYC Open Data API."""
    if app_token is None:
        app_token = os.environ.get("SOCRATA_APP_TOKEN")
    
    ingestion_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(out) / f"ingested_at={ingestion_ts}"

    all_rows: list[dict] = []
    with requests.Session() as session:
        for page in range(max_pages):
            offset = page * limit
            rows = fetch_page(
                session=session,
                limit=limit,
                offset=offset,
                where=where,
                app_token=app_token,
            )
            if not rows:
                break
            all_rows.extend(rows)

    if not all_rows:
        raise SystemExit("No rows returned. Try to increase max pages or adjust the date filter.")

    df = pd.DataFrame(all_rows)
    df["_ingested_at_utc"] = ingestion_ts

    out_file = write_parquet(df, out_dir)
    print(f"Wrote {len(df):,} rows to {out_file}")
    return out_file