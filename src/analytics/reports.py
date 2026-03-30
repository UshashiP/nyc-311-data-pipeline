"""
Analytics module for NYC 311 data pipeline.
Runs analytical queries against the star schema and exports reports as CSVs.
"""
import duckdb
import pandas as pd
from pathlib import Path


QUERIES = {
    "avg_response_time_by_borough": """
        SELECT
            loc.borough,
            ROUND(AVG(fact.response_time_hours), 2) AS avg_hours,
            COUNT(*) AS n_requests
        FROM fact
        JOIN loc ON fact.location_id = loc.location_id
        WHERE fact.response_time_hours IS NOT NULL
          AND fact.response_time_hours >= 0
          AND loc.borough IS NOT NULL
        GROUP BY 1
        ORDER BY avg_hours DESC
    """,
    "top_20_complaint_types": """
        SELECT
            complaint.complaint_type,
            COUNT(*) AS n_requests
        FROM fact
        JOIN complaint ON fact.complaint_id = complaint.complaint_id
        GROUP BY 1
        ORDER BY n_requests DESC
        LIMIT 20
    """,
    "top_agencies_by_volume": """
        SELECT
            agency.agency,
            COUNT(*) AS n_requests
        FROM fact
        JOIN agency ON fact.agency_id = agency.agency_id
        GROUP BY 1
        ORDER BY n_requests DESC
        LIMIT 15
    """,
    "monthly_volume_trend": """
        SELECT
            EXTRACT(year  FROM created_date) AS year,
            EXTRACT(month FROM created_date) AS month,
            COUNT(*) AS n_requests
        FROM fact
        WHERE created_date IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "slowest_complaints_by_avg_time": """
        SELECT
            complaint.complaint_type,
            ROUND(AVG(fact.response_time_hours), 2) AS avg_hours,
            COUNT(*) AS n_requests
        FROM fact
        JOIN complaint ON fact.complaint_id = complaint.complaint_id
        WHERE fact.response_time_hours IS NOT NULL
          AND fact.response_time_hours >= 0
        GROUP BY 1
        HAVING COUNT(*) >= 100
        ORDER BY avg_hours DESC
        LIMIT 20
    """,
    "data_quality_null_rates": """
        SELECT
            ROUND(100.0 * SUM(CASE WHEN created_date  IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_created_null,
            ROUND(100.0 * SUM(CASE WHEN complaint_id  IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_complaint_fk_null,
            ROUND(100.0 * SUM(CASE WHEN agency_id     IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_agency_fk_null,
            ROUND(100.0 * SUM(CASE WHEN location_id   IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_location_fk_null
        FROM fact
    """,
}


def generate_reports(
    analytics_dir: Path,
    reports_dir: Path,
) -> dict:
    """
    Runs analytical queries against the star schema and saves results as CSVs.

    Args:
        analytics_dir: Directory containing star schema Parquet files.
        reports_dir: Directory to write CSV reports.

    Returns:
        Dict mapping report name to resulting DataFrame.

    Raises:
        FileNotFoundError: If any required analytics Parquet files are missing.
    """
    analytics_dir = Path(analytics_dir)
    reports_dir = Path(reports_dir)

    required = [
        "fact_311_requests.parquet",
        "dim_agency.parquet",
        "dim_complaint.parquet",
        "dim_location.parquet",
        "dim_date.parquet",
    ]
    missing = [f for f in required if not (analytics_dir / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing analytics files: {missing}. "
            "Run the star schema build step first."
        )

    reports_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE VIEW fact      AS SELECT * FROM read_parquet('{analytics_dir / 'fact_311_requests.parquet'}')")
    con.execute(f"CREATE VIEW agency    AS SELECT * FROM read_parquet('{analytics_dir / 'dim_agency.parquet'}')")
    con.execute(f"CREATE VIEW complaint AS SELECT * FROM read_parquet('{analytics_dir / 'dim_complaint.parquet'}')")
    con.execute(f"CREATE VIEW loc       AS SELECT * FROM read_parquet('{analytics_dir / 'dim_location.parquet'}')")
    con.execute(f"CREATE VIEW ddate     AS SELECT * FROM read_parquet('{analytics_dir / 'dim_date.parquet'}')")

    results = {}
    for name, sql in QUERIES.items():
        df = con.execute(sql).df()
        df.to_csv(reports_dir / f"{name}.csv", index=False)
        results[name] = df

    con.close()
    return results