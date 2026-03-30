"""
Analytics module for NYC 311 data pipeline
Builds a star schema (fact + dimension tables) from cleaned Parquet data
"""
import duckdb
from pathlib import Path


def _sql_str(s: str) -> str:
    """Escape a string for safe insertion into DuckDB SQL string literals."""
    return s.replace("'", "''")


def build_star_schema(
    clean_dir: Path,
    analytics_dir: Path,
) -> dict:
    """
    Builds a star schema from cleaned 311 Parquet data and writes
    dimension + fact tables as Parquet files.

    Args:
        clean_dir: Directory containing cleaned Parquet files.
        analytics_dir: Directory to write star schema Parquet files.

    Returns:
        Dict with row counts for each table.

    Raises:
        FileNotFoundError: If clean_dir does not exist.
    """
    clean_dir = Path(clean_dir)
    analytics_dir = Path(analytics_dir)

    if not clean_dir.exists():
        raise FileNotFoundError(
            f"Clean data directory not found: {clean_dir}. "
            "Run the transformation step first."
        )

    analytics_dir.mkdir(parents=True, exist_ok=True)

    parquet_glob = _sql_str(str(clean_dir / "**" / "*.parquet"))

    con = duckdb.connect(database=":memory:")

    con.execute(f"""
        CREATE OR REPLACE VIEW clean_311 AS
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)

    # ── Dimension tables ───────────────────────────────────────────────────

    con.execute("""
        CREATE OR REPLACE TABLE dim_agency AS
        SELECT
            dense_rank() OVER (ORDER BY agency) AS agency_id,
            agency
        FROM (
            SELECT DISTINCT agency
            FROM clean_311
            WHERE agency IS NOT NULL AND agency <> ''
        )
        ORDER BY agency
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dim_complaint AS
        SELECT
            dense_rank() OVER (ORDER BY complaint_type, descriptor) AS complaint_id,
            complaint_type,
            descriptor
        FROM (
            SELECT DISTINCT complaint_type, descriptor
            FROM clean_311
            WHERE complaint_type IS NOT NULL AND complaint_type <> ''
        )
        ORDER BY complaint_type, descriptor
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dim_location AS
        SELECT
            dense_rank() OVER (ORDER BY borough, incident_zip, latitude, longitude) AS location_id,
            borough,
            incident_zip,
            latitude,
            longitude
        FROM (
            SELECT DISTINCT borough, incident_zip, latitude, longitude
            FROM clean_311
        )
        ORDER BY borough, incident_zip, latitude, longitude
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            CAST(created_date AS DATE)          AS date,
            EXTRACT(year  FROM created_date)    AS year,
            EXTRACT(month FROM created_date)    AS month,
            EXTRACT(day   FROM created_date)    AS day,
            EXTRACT(dow   FROM created_date)    AS day_of_week
        FROM clean_311
        WHERE created_date IS NOT NULL
    """)

    # ── Fact table ─────────────────────────────────────────────────────────

    con.execute("""
        CREATE OR REPLACE TABLE fact_311_requests AS
        SELECT
            c.unique_key,
            c.created_date,
            c.closed_date,
            c.response_time_hours,
            a.agency_id,
            comp.complaint_id,
            loc.location_id
        FROM clean_311 c
        LEFT JOIN dim_agency a
            ON c.agency = a.agency
        LEFT JOIN dim_complaint comp
            ON c.complaint_type = comp.complaint_type
           AND c.descriptor = comp.descriptor
        LEFT JOIN dim_location loc
            ON c.borough = loc.borough
           AND c.incident_zip = loc.incident_zip
           AND c.latitude IS NOT DISTINCT FROM loc.latitude
           AND c.longitude IS NOT DISTINCT FROM loc.longitude
    """)

    # ── Write Parquet outputs ──────────────────────────────────────────────

    tables = ["dim_agency", "dim_complaint", "dim_location", "dim_date", "fact_311_requests"]
    for table in tables:
        out_path = _sql_str(str(analytics_dir / f"{table}.parquet"))
        con.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET)")

    # ── Row count summary ──────────────────────────────────────────────────

    counts = {
        table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in tables
    }

    con.close()
    return counts