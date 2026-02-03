from pathlib import Path
import duckdb


ANALYTICS_BASE = Path("data/analytics/nyc_311")
REPORTS_BASE = Path("reports")


def execute_queries():
    """Execute analytical queries and generate reports."""
    required = [
        "fact_311_requests.parquet",
        "dim_agency.parquet",
        "dim_complaint.parquet",
        "dim_location.parquet",
        "dim_date.parquet",
    ]
    missing = [f for f in required if not (ANALYTICS_BASE / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing analytics files: {missing}")

    REPORTS_BASE.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")

    con.execute(f"CREATE VIEW fact AS SELECT * FROM read_parquet('{ANALYTICS_BASE / 'fact_311_requests.parquet'}')")
    con.execute(f"CREATE VIEW agency AS SELECT * FROM read_parquet('{ANALYTICS_BASE / 'dim_agency.parquet'}')")
    con.execute(f"CREATE VIEW complaint AS SELECT * FROM read_parquet('{ANALYTICS_BASE / 'dim_complaint.parquet'}')")
    con.execute(f"CREATE VIEW loc AS SELECT * FROM read_parquet('{ANALYTICS_BASE / 'dim_location.parquet'}')")
    con.execute(f"CREATE VIEW ddate AS SELECT * FROM read_parquet('{ANALYTICS_BASE / 'dim_date.parquet'}')")

    queries = {
        "avg_response_time_by_borough": """
            SELECT
                loc.borough,
                ROUND(AVG(fact.response_time_minutes), 2) AS avg_minutes,
                COUNT(*) AS n_requests
            FROM fact
            JOIN loc ON fact.location_id = loc.location_id
            WHERE fact.response_time_minutes IS NOT NULL
              AND fact.response_time_minutes >= 0
              AND loc.borough IS NOT NULL
            GROUP BY 1
            ORDER BY avg_minutes DESC
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
                EXTRACT(year FROM created_date) AS year,
                EXTRACT(month FROM created_date) AS month,
                COUNT(*) AS n_requests
            FROM fact
            WHERE created_date IS NOT NULL
            GROUP BY 1,2
            ORDER BY 1,2
        """,
        "slowest_complaints_by_avg_time": """
            SELECT
                complaint.complaint_type,
                ROUND(AVG(fact.response_time_minutes), 2) AS avg_minutes,
                COUNT(*) AS n_requests
            FROM fact
            JOIN complaint ON fact.complaint_id = complaint.complaint_id
            WHERE fact.response_time_minutes IS NOT NULL
              AND fact.response_time_minutes >= 0
            GROUP BY 1
            HAVING COUNT(*) >= 100
            ORDER BY avg_minutes DESC
            LIMIT 20
        """,
        "data_quality_null_rates": """
            SELECT
                ROUND(100.0 * SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_created_null,
                ROUND(100.0 * SUM(CASE WHEN complaint_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_complaint_fk_null,
                ROUND(100.0 * SUM(CASE WHEN agency_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_agency_fk_null,
                ROUND(100.0 * SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_location_fk_null
            FROM fact
        """,
    }

    for name, sql in queries.items():
        df = con.execute(sql).df()
        out_csv = REPORTS_BASE / f"{name}.csv"
        df.to_csv(out_csv, index=False)

        print(name)
        print(df.head(20).to_string(index=False))
        print(f"Saved → {out_csv}")

    print("Step 4 complete: queries ran and reports saved in /reports")