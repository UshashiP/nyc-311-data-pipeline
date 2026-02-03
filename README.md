# NYC 311 Service Requests Data Pipeline

This is a Python data pipeline for ingesting, transforming, and analyzing NYC 311 service request data from the NYC Open Data API.

## Overview

This pipeline automates the ETL process for NYC 311 data:
1. **Ingest** raw data from NYC Open Data API (Socrata)
2. **Transform** and clean the data with validation
3. **Build** analytics-ready star schema
4. **Query** and generate reports with insights

## Features

- **Incremental data ingestion** with timestamp partitioning
- **Data quality checks** and cleaning
- **Star schema** design for analytics (fact + dimension tables)
- **Pre-built analytical queries** for common insights
- **Parquet format** for efficient storage
- **DuckDB** for fast analytical queries
- **Portable** - works on any system after cloning


### Prerequisites

- Python 3.8+
- pip

### Installation

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd nyc-311-data-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the pipeline**
   ```bash
   python main.py
   ```

That's it! The pipeline will create all necessary directories and data files.

### Optional: Socrata App Token

For higher API rate limits, get a free token from [NYC Open Data](https://data.cityofnewyork.us/login) and set:

```bash
export SOCRATA_APP_TOKEN=your_token_here
python main.py
```

## Project Structure

```
nyc-311-data-pipeline/
├── main.py                      # Entry point - orchestrates all steps
├── pipelines/
│   ├── __init__.py
│   ├── nyc311_data.py          # Step 1: Data ingestion
│   ├── transform.py            # Step 2: Data cleaning
│   ├── build_analytics.py      # Step 3: Star schema builder
│   └── run_queries.py          # Step 4: Analytics queries
├── data/                        # Generated data (gitignored)
│   ├── raw/nyc_311/            # Raw ingested data
│   ├── clean/nyc_311/          # Cleaned & partitioned data
│   └── analytics/nyc_311/      # Star schema tables
├── reports/                     # Generated CSV reports
├── requirements.txt
└── README.md
```

## Pipeline Steps

### Step 1: Data Ingestion
Fetches raw data from NYC 311 API and writes timestamped Parquet files:
```python
from pipelines.nyc311_data import ingest_raw_data
ingest_raw_data(limit=50000, max_pages=1)
```

### Step 2: Data Transformation
Cleans and validates data, partitioned by year/month:
- Date parsing and validation
- Coordinate normalization
- Borough standardization
- Duplicate removal
- Response time calculation

### Step 3: Analytics Schema
Builds star schema with:
- **Fact table**: `fact_311_requests`
- **Dimensions**: `dim_agency`, `dim_complaint`, `dim_location`, `dim_date`

### Step 4: Query Execution
Generates analytical reports:
- Average response time by borough
- Top 20 complaint types
- Top agencies by volume
- Monthly volume trends
- Slowest complaints by response time
- Data quality metrics

## Sample Queries

All reports are saved as CSV files in `reports/`:

```bash
reports/
├── avg_response_time_by_borough.csv
├── top_20_complaint_types.csv
├── top_agencies_by_volume.csv
├── monthly_volume_trend.csv
├── slowest_complaints_by_avg_time.csv
└── data_quality_null_rates.csv
```

## Advanced Usage

### Run Individual Steps

```bash
# Only ingest data
python pipelines/nyc311_data.py -max-pages 5

# Only transform
python pipelines/transform.py

# Only build analytics
python pipelines/build_analytics.py

# Only run queries
python pipelines/run_queries.py
```

### Custom Date Filters

```bash
python pipelines/nyc311_data.py \
  -where "created_date >= '2024-01-01T00:00:00.000'" \
  -max-pages 10
```

### Adjust Page Size

```bash
python pipelines/nyc311_data.py -limit 10000 -max-pages 100
```

## Dependencies

- **pandas**: Data manipulation
- **requests**: API calls
- **pyarrow**: Parquet file format
- **duckdb**: Fast analytical queries

See `requirements.txt` for complete list.

## Data Source

This pipeline uses the [NYC 311 Service Requests dataset](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) from NYC Open Data.

- **Dataset**: 311 Service Requests from 2010 to Present
- **Resource ID**: `erm2-nwe9`
- **API**: Socrata Open Data API (SODA)
- **Update Frequency**: Daily

For questions or issues, please open an issue on GitHub! :)
