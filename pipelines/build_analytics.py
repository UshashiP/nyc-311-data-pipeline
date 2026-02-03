from pathlib import Path
import duckdb

CLEAN_BASE = Path("data/clean/nyc_311")
ANALYTICS_BASE = Path("data/analytics/nyc_311")


def sql_str(s: str) -> str:
    """Escape a Python string for safe insertion into DuckDB SQL string literals."""
    return s.replace("'", "''")


def build_star_schema():
    """Build analytics star schema from clean data."""
    if not CLEAN_BASE.exists():
        raise FileNotFoundError(
            "Clean layer not found. Run Step 2 first to create data/clean/nyc_311/"
        )

    ANALYTICS_BASE.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")

    parquet_glob = str(CLEAN_BASE / "**" / "*.parquet")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW clean_311 AS
        SELECT * FROM read_parquet('{sql_str(parquet_glob)}')
        """
    )

    # Agency dimension
    con.execute(
        """
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
        """
    )

    # Complaint dimension
    con.execute(
        """
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
        """
    )

    # Location dimension
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_location AS
        SELECT
            dense_rank() OVER (ORDER BY borough, incident_zip, latitude, longitude) AS location_id,
            borough,
            incident_zip,
            latitude,
            longitude
        FROM (
            SELECT DISTINCT
                borough,
                incident_zip,
                latitude,
                longitude
            FROM clean_311
        )
        ORDER BY borough, incident_zip, latitude, longitude
        """
    )

    # Date dimension
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            CAST(created_date AS DATE) AS date,
            EXTRACT(year FROM created_date) AS year,
            EXTRACT(month FROM created_date) AS month,
            EXTRACT(day FROM created_date) AS day,
            EXTRACT(dow FROM created_date) AS day_of_week
        FROM clean_311
        WHERE created_date IS NOT NULL
        """
    )

    # Fact table
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_311_requests AS
        SELECT
            c.unique_key,
            c.created_date,
            c.closed_date,
            c.response_time_minutes,

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
        """
    )

    # Write outputs
    con.execute(
        f"COPY dim_agency TO '{sql_str(str(ANALYTICS_BASE / 'dim_agency.parquet'))}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY dim_complaint TO '{sql_str(str(ANALYTICS_BASE / 'dim_complaint.parquet'))}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY dim_location TO '{sql_str(str(ANALYTICS_BASE / 'dim_location.parquet'))}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY dim_date TO '{sql_str(str(ANALYTICS_BASE / 'dim_date.parquet'))}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY fact_311_requests TO '{sql_str(str(ANALYTICS_BASE / 'fact_311_requests.parquet'))}' (FORMAT PARQUET)"
    )

    total = con.execute("SELECT COUNT(*) FROM fact_311_requests").fetchone()[0]
    agencies = con.execute("SELECT COUNT(*) FROM dim_agency").fetchone()[0]
    complaints = con.execute("SELECT COUNT(*) FROM dim_complaint").fetchone()[0]
    locations = con.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0]
    dates = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]

    print("Step 3 complete")
    print(f"fact_311_requests rows: {total:,}")
    print(f"dim_agency rows: {agencies:,}")
    print(f"dim_complaint rows: {complaints:,}")
    print(f"dim_location rows: {locations:,}")
    print(f"dim_date rows: {dates:,}")
    print(f"Wrote analytics parquet to: {ANALYTICS_BASE}")