# NYC 311 Data Pipeline

A production-grade, end-to-end data engineering pipeline that ingests, transforms, validates, and analyzes NYC 311 service request data from the NYC Open Data API.

Built with Python, DuckDB, and Parquet — a modular, config-driven workflow, tested with CI/CD via GitHub Actions.

---

## What is this?

NYC 311 is New York City's non-emergency service request system. Every day, thousands of requests are submitted for issues like noise complaints, graffiti, heat outages, and more.

This pipeline:
1. **Ingests** raw 311 data from the [NYC Open Data API](https://data.cityofnewyork.us/resource/erm2-nwe9.json) with pagination and retry logic
2. **Transforms** and cleans the data — standardizing boroughs, parsing dates, normalizing coordinates, and  imputing missing values
3. **Validates** data quality — checking schema, null rates, duplicates, and date logic
4. **Loads** cleaned data to DuckDB and Parquet for efficient querying
5. **Builds** a star schema (fact + dimension tables) for analytics
6. **Generates** analytical reports as CSV files

---

## Use Cases

- **City analysts** — understand which boroughs have the slowest response times, 
  which complaint types are most common, and how request volume trends over time
- **Data engineers** — reference implementation of a modular ETL pipeline with 
  testing, validation, and CI/CD
- **Data scientists** — analytics-ready star schema and Parquet files ready for 
  modeling or visualization in Tableau, Power BI, or Python
- **Developers** — extend the pipeline to add new data sources, transformations, 
  or load to a cloud warehouse (BigQuery, Snowflake, Redshift)

  ---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.9+ | Core pipeline language |
| Pandas | Data transformation |
| DuckDB | Fast analytical queries and star schema |
| PyArrow | Parquet file format |
| Requests + urllib3 | API ingestion with retry logic |
| PyYAML | Config management |
| Pytest | Unit testing |
| GitHub Actions | CI/CD |

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    run_pipeline.py                          │
│                  (orchestrates all steps)                   │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   INGESTION     │────▶│  TRANSFORMATION  │────▶│   VALIDATION     │
│  api_client.py  │     │   cleaner.py     │     │  validator.py    │
│                 │     │                  │     │                  │
│ • Pagination    │     │ • Date parsing   │     │ • Schema check   │
│ • Retry logic   │     │ • Coord normalize│     │ • Null rates     │
│ • Date filters  │     │ • Borough std.   │     │ • Duplicates     │
│ • Save to disk  │     │ • Smart null fill│     │ • Date logic     │
└─────────────────┘     └──────────────────┘     └──────────────────┘
                                                          │
           ┌──────────────────────────────────────────────┘
           ▼
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│    LOADING      │────▶│   STAR SCHEMA    │────▶│    REPORTS       │
│   loader.py     │     │ star_schema.py   │     │   reports.py     │
│                 │     │                  │     │                  │
│ • DuckDB        │     │ • fact_requests  │     │ • Response time  │
│ • Parquet       │     │ • dim_agency     │     │ • Top complaints │
│                 │     │ • dim_complaint  │     │ • Agency volume  │
│                 │     │ • dim_location   │     │ • Monthly trends │
│                 │     │ • dim_date       │     │ • Data quality   │
└─────────────────┘     └──────────────────┘     └──────────────────┘
```

---

## Project Structure

```
nyc-311-data-pipeline/
│
├── run_pipeline.py              # Main entry point
├── requirements.txt
├── .gitignore
├── README.md
│
├── config/
│   └── pipeline_config.yaml    # All pipeline settings
│
├── src/
│   ├── ingestion/
│   │   └── api_client.py       # Fetch from NYC Open Data API
│   ├── transformation/
│   │   └── cleaner.py          # Clean and transform data
│   ├── validation/
│   │   └── validator.py        # Data quality checks
│   ├── loading/
│   │   └── loader.py           # Save to DuckDB and Parquet
│   ├── analytics/
│   │   ├── star_schema.py      # Build fact + dimension tables
│   │   └── reports.py          # Run queries, export CSVs
│   └── utils/
│       ├── config.py           # Load pipeline_config.yaml
│       └── logger.py           # Logging setup
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   ├── test_validator.py
│   ├── test_loader.py
│   ├── test_analytics.py
│   └── test_config.py
│
├── data/                        # Generated at runtime (gitignored)
│   ├── raw/nyc_311/            # Raw API data
│   ├── clean/nyc_311/          # Cleaned Parquet
│   ├── analytics/nyc_311/      # Star schema Parquet files
│   └── db/                     # DuckDB database
│
├── logs/                        # Pipeline logs (gitignored)
└── reports/                     # CSV reports (gitignored)
```

---

## Getting Started

### Prerequisites
- Python 3.9+
- pip

### Installation

1. Clone the repository
```bash
git clone <repo-url>
cd nyc-311-data-pipeline
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. (Optional) Set Socrata app token for higher API rate limits
```bash
export SOCRATA_APP_TOKEN=your_token_here
```
Get a free token at [NYC Open Data](https://data.cityofnewyork.us/login).

### Run the Pipeline

```bash
python run_pipeline.py
```

That's it. The pipeline will:
- Create all necessary directories automatically
- Fetch 50,000 records from the NYC 311 API
- Clean, validate, and load the data
- Build a star schema and generate reports

### Configuration

All settings are in `config/pipeline_config.yaml`:

```yaml
api:
  total_limit: 50000       # number of records to fetch
  batch_size: 1000         # records per API request
  start_date: "2025-01-01" # date range filter
  end_date: "2025-03-30"

cleaning:
  drop_duplicates: true

output:
  table_name: "nyc_311_cleaned"
```

---

## Output

### Data Files
After running the pipeline, the following files are created:

| File | Description |
|---|---|
| `data/raw/nyc_311/nyc_311_raw.parquet` | Raw API data |
| `data/clean/nyc_311/nyc_311_clean.parquet` | Cleaned data |
| `data/db/nyc_311.duckdb` | DuckDB database |
| `data/analytics/nyc_311/*.parquet` | Star schema tables |

### Reports
Six analytical reports are saved to `reports/`:

| Report | Description |
|---|---|
| `avg_response_time_by_borough.csv` | Average resolution time per borough |
| `top_20_complaint_types.csv` | Most common complaint types |
| `top_agencies_by_volume.csv` | Agencies handling most requests |
| `monthly_volume_trend.csv` | Request volume over time |
| `slowest_complaints_by_avg_time.csv` | Complaint types with longest resolution |
| `data_quality_null_rates.csv` | Null rates across key columns |

---

## Running Tests

```bash
pytest tests/ -v
```

Tests cover all modules with mocked API calls — no network access required.

---

## CI/CD

GitHub Actions runs the full test suite on every push and pull request across Python 3.9, 3.10, and 3.11.

---

## Data Source

- **Dataset**: [311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9)
- **Provider**: NYC Open Data
- **API**: Socrata Open Data API (SODA)
- **Update Frequency**: Daily

For questions or issues, please open an issue on GitHub! :)

