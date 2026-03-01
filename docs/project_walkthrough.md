# Project Walkthrough: Why and How

This document explains the architectural decisions behind this mini data platform, written from the perspective of a senior data engineer mentoring a junior team member.

---

## Quick Start: Hands-On Tutorial

This section walks you through the entire project step-by-step. Follow along to understand how data flows through the system.

### Prerequisites

Make sure Docker Desktop is running and you're in the project root folder:

```powershell
cd c:\Users\ZainabAbdullai\Desktop\mini-data-platform
```

### Step 1: Start All Services

```powershell
docker-compose up -d
```

Wait 60 seconds for all services to initialize. Check they're running:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

You should see 7 containers running:
- `minio` - Object storage (ports 9000, 9001)
- `airflow-webserver` - DAG UI (port 8080)
- `airflow-scheduler` - Runs tasks
- `airflow-db` - Airflow metadata
- `analytics-db` - Your sales data (port 5433)
- `metabase-db` - Metabase metadata
- `metabase` - Dashboards (port 3000)

### Step 2: Access the Web UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **Airflow** | http://localhost:8080 | `admin` / `admin` |
| **Metabase** | http://localhost:3000 | Set up on first visit |

Open each in your browser to verify they're working.

### Step 3: Generate Test Data

Open a terminal and generate 100 sales records:

```powershell
cd data-generator
pip install -r requirements.txt
python generate_sales.py --records 100 --upload
```

This does two things:
1. Creates a CSV file in `data/output/` with 100 fake sales records (5% have intentional data quality issues)
2. Uploads it to MinIO's `raw-sales` bucket

**Verify in MinIO Console:**
1. Go to http://localhost:9001
2. Login with `minioadmin` / `minioadmin`
3. Click **Object Browser** → **raw-sales**
4. You should see your CSV file (e.g., `sales_data_20260227_095123.csv`)

### Step 4: Run the Pipeline

**Option A: Trigger manually via CLI**
```powershell
docker exec -u airflow airflow-webserver airflow dags trigger sales_data_pipeline
```

**Option B: Trigger via Airflow UI**
1. Go to http://localhost:8080
2. Login with `admin` / `admin`
3. Find `sales_data_pipeline` in the DAG list
4. Toggle the switch to **ON** (unpause)
5. Click the **Play button** (▶) → **Trigger DAG**

**Watch the pipeline run:**
- In Airflow UI, click on the DAG name → **Graph** view
- Tasks turn green as they complete (takes ~30 seconds)

### Step 5: Verify the Results

**Check MinIO - Archive bucket:**

The raw file is now archived (moved from `raw-sales` to `archive`):

```powershell
docker exec -u airflow airflow-webserver python -c "
from minio import Minio
client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
print('=== RAW-SALES (should be empty) ===')
for obj in client.list_objects('raw-sales'): print(f'  {obj.object_name}')
print('=== ARCHIVE (your processed files) ===')
for obj in client.list_objects('archive', recursive=True): print(f'  {obj.object_name}')
"
```

**Check PostgreSQL - Your data is loaded:**

```powershell
# Count total rows
docker exec analytics-db psql -U analytics -d analytics -c "SELECT COUNT(*) FROM sales;"

# See sample data
docker exec analytics-db psql -U analytics -d analytics -c "SELECT order_id, product_name, quantity, unit_price, total_amount, country FROM sales LIMIT 10;"

# Revenue by country
docker exec analytics-db psql -U analytics -d analytics -c "SELECT country, COUNT(*) as orders, ROUND(SUM(total_amount)::numeric, 2) as revenue FROM sales GROUP BY country ORDER BY revenue DESC;"
```

### Step 6: Use Metabase for Dashboards

1. Go to http://localhost:3000
2. **First time setup:**
   - Click "Let's get started"
   - Fill in your details (any email/password for local use)
   - Select "I'll add my data later" → Skip
3. **Add the analytics database:**
   - Go to ⚙️ **Settings** → **Admin settings** → **Databases** → **Add database**
   - Database type: **PostgreSQL**
   - Display name: `Analytics`
   - Host: `analytics-db`
   - Port: `5432`
   - Database name: `analytics`
   - Username: `analytics`
   - Password: `analytics`
   - Click **Save**
4. **Sync the schema:**
   - Click on your new database
   - Click **Sync database schema now**

**Create your first visualization:**

1. Click **+ New** → **Question**
2. Select **Analytics** database → **Sales** table
3. Click **Pick columns to group by** → select `country`
4. Click **Pick a metric** → **Sum of...** → `total_amount`
5. Click **Visualize**
6. Change chart type to **Bar chart** using the icon at bottom left
7. Click **Save** → name it "Revenue by Country"

**Create a dashboard:**

1. Click **+ New** → **Dashboard**
2. Name it "Sales Overview"
3. Click **+** to add questions (your saved visualizations)
4. Save the dashboard

### Step 7: Run the Pipeline Again

Generate more data and re-run to see the database grow:

```powershell
cd data-generator
python generate_sales.py --records 200 --upload
```

Wait 15 minutes for the scheduled run, OR trigger manually:

```powershell
docker exec -u airflow airflow-webserver airflow dags trigger sales_data_pipeline
```

Check the new totals:

```powershell
docker exec analytics-db psql -U analytics -d analytics -c "SELECT COUNT(*) as total_rows, ROUND(SUM(total_amount)::numeric, 2) as total_revenue FROM sales;"
```

Refresh your Metabase dashboard to see updated charts!

---

## How to Test the CI Pipeline

The project includes a GitHub Actions CI workflow (`.github/workflows/ci.yml`). Here's how to test it:

### Run Tests Locally (Same as CI)

```powershell
# From project root
cd c:\Users\ZainabAbdullai\Desktop\mini-data-platform

# Install test dependencies
pip install -r tests/requirements.txt

# Run all tests with coverage
pytest tests/ -v --cov=airflow/dags --cov-report=term-missing

# Run specific test files
pytest tests/test_schemas.py -v        # Pandera schema tests
pytest tests/test_data_cleaner.py -v   # Data cleaning tests
pytest tests/test_integration.py -v    # End-to-end tests
```

### What CI Does on Every Push

When you push to GitHub, the workflow runs:

1. **Lint** - Checks code style with flake8
2. **Test** - Runs pytest with coverage report
3. **DAG Validation** - Ensures the Airflow DAG imports without errors
4. **Build** - Builds Docker images to catch Dockerfile issues

### Trigger CI Manually

1. Push your code to GitHub:
   ```powershell
   git add .
   git commit -m "Test CI pipeline"
   git push origin main
   ```

2. Go to your GitHub repo → **Actions** tab
3. Watch the workflow run

### Test DAG Import Locally

Before pushing, verify the DAG loads correctly:

```powershell
docker exec -u airflow airflow-webserver python -c "
import sys
sys.path.insert(0, '/opt/airflow/dags')
from sales_pipeline import dag
print(f'DAG loaded: {dag.dag_id}')
print(f'Tasks: {[t.task_id for t in dag.tasks]}')
"
```

---

## Understanding the Data Flow

Here's what happens when you run `python generate_sales.py --records 100 --upload`:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. GENERATE                     2. UPLOAD                                   │
│  ┌─────────────────┐            ┌─────────────────┐                         │
│  │ generate_sales  │  ───────>  │ MinIO           │                         │
│  │ .py             │  CSV       │ raw-sales/      │                         │
│  └─────────────────┘            └────────┬────────┘                         │
│         │                                │                                   │
│         │ also saves to                  │ Airflow detects                   │
│         ▼                                ▼                                   │
│  ┌─────────────────┐            ┌─────────────────┐                         │
│  │ data/output/    │            │ detect_new_files│                         │
│  │ (local backup)  │            └────────┬────────┘                         │
│  └─────────────────┘                     │                                   │
│                                          ▼                                   │
│                                 ┌─────────────────┐                         │
│                                 │ validate_schema │  Pandera: RawSalesSchema │
│                                 └────────┬────────┘                         │
│                                          │                                   │
│                                          ▼                                   │
│                                 ┌─────────────────┐                         │
│                                 │ clean_data      │  Remove duplicates,      │
│                                 │                 │  fix types, fill nulls   │
│                                 └────────┬────────┘                         │
│                                          │                                   │
│                                          ▼                                   │
│                                 ┌─────────────────┐                         │
│                                 │ compute_totals  │  total_amount =          │
│                                 │                 │  quantity × unit_price   │
│                                 └────────┬────────┘                         │
│                                          │                                   │
│                           ┌──────────────┼──────────────┐                   │
│                           ▼              ▼              ▼                   │
│                  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│                  │ load_to_   │  │ archive_   │  │ generate_  │            │
│                  │ postgres   │  │ processed  │  │ summary    │            │
│                  └─────┬──────┘  └─────┬──────┘  └────────────┘            │
│                        │               │                                    │
│                        ▼               ▼                                    │
│                  ┌────────────┐  ┌────────────┐                            │
│                  │ PostgreSQL │  │ MinIO      │                            │
│                  │ sales table│  │ archive/   │                            │
│                  └─────┬──────┘  └────────────┘                            │
│                        │                                                    │
│                        ▼                                                    │
│                  ┌────────────┐                                            │
│                  │ Metabase   │  Visualize with charts & dashboards        │
│                  └────────────┘                                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Folder Structure Explained

```
mini-data-platform/
├── data/
│   └── output/                  # Generated CSV files (local backup)
├── data-generator/
│   ├── generate_sales.py        # Creates fake sales data
│   └── requirements.txt         # minio, pandas
├── airflow/
│   ├── dags/
│   │   ├── sales_pipeline.py    # The main DAG (task definitions)
│   │   ├── utils/               # Logging & connections
│   │   ├── validation/          # Pandera schemas
│   │   ├── services/            # MinIO & PostgreSQL clients
│   │   └── transformers/        # Data cleaning & enrichment
│   └── logs/                    # Pipeline logs written here
├── sql/
│   └── init.sql                 # Creates the "sales" table
├── tests/                       # pytest test suite
├── docs/                        # This walkthrough + other docs
├── .github/workflows/
│   └── ci.yml                   # GitHub Actions CI pipeline
└── docker-compose.yml           # All service definitions
```

---

## Common Commands Reference

```powershell
# Start everything
docker-compose up -d

# Stop everything (keeps data)
docker-compose down

# Stop everything and DELETE all data
docker-compose down -v

# View logs
docker logs airflow-scheduler --tail 50
docker logs airflow-webserver --tail 50

# Generate data
cd data-generator
python generate_sales.py --records 100 --upload

# Trigger pipeline
docker exec -u airflow airflow-webserver airflow dags trigger sales_data_pipeline

# Check pipeline status
docker exec -u airflow airflow-webserver airflow dags list-runs -d sales_data_pipeline

# Query PostgreSQL
docker exec analytics-db psql -U analytics -d analytics -c "SELECT COUNT(*) FROM sales;"

# Run tests
pytest tests/ -v

# Reset Airflow user if login fails
docker exec -u airflow airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

---

## Why This Architecture?

### The Business Problem

Imagine you're a growing e-commerce company. Every day, your sales systems generate transaction data:
- Orders come from multiple channels (website, mobile app, partners)
- Data arrives in different formats and quality levels
- Business teams need daily reports on sales performance
- Analysts need clean, queryable data for ad-hoc analysis

You need a system that:
1. Accepts raw data from various sources
2. Validates and cleans it automatically
3. Stores it in a format optimized for analysis
4. Presents insights through dashboards

This project solves that problem at a small scale, using the same patterns that large companies use.

---

## Why Each Tool Was Chosen

### MinIO (Object Storage)

**What it does:** Stores raw files before processing

**Why MinIO specifically:**
- S3-compatible API (industry standard)
- Runs locally in Docker (no cloud account needed)
- Free and open-source
- Same interface as Amazon S3 (skills transfer directly)

**Why object storage instead of a database:**
- Raw files should be preserved exactly as received
- Files can be any format (CSV, JSON, Parquet)
- Cheap storage for large volumes
- Easy to reprocess historical data

**Real-world parallel:** Production systems use Amazon S3, Google Cloud Storage, or Azure Blob Storage. MinIO lets you learn the patterns without cloud costs.

### Apache Airflow (Orchestration)

**What it does:** Schedules and monitors data pipeline execution

**Why Airflow specifically:**
- Industry standard for batch orchestration
- Visual DAG representation
- Built-in retry logic and alerting
- Active open-source community
- Used by Airbnb, Lyft, Twitter, and thousands of companies

**Why not just a cron job:**
- Cron doesn't track task dependencies
- Cron doesn't provide retry on failure
- Cron doesn't give you visibility into what ran
- Cron can't pause/resume pipelines

**Real-world parallel:** You'll encounter Airflow in most data engineering roles. Alternatives include Prefect, Dagster, and Luigi, but Airflow dominates job listings.

### PostgreSQL (Analytics Database)

**What it does:** Stores clean, structured data for querying

**Why PostgreSQL specifically:**
- Rock-solid reliability (30+ years of development)
- Excellent SQL compliance
- Free and open-source
- Great performance for analytical queries
- Supports indexes for fast lookups

**Why not keep data in files:**
- SQL enables complex queries without code
- Indexes make lookups fast
- Transactions ensure data consistency
- BI tools expect database connections

**Real-world parallel:** Production analytics often uses data warehouses (Snowflake, BigQuery, Redshift), but PostgreSQL is excellent for learning and small-scale analytics.

### Metabase (Dashboards)

**What it does:** Creates visual dashboards from database data

**Why Metabase specifically:**
- Simple setup and intuitive interface
- Free open-source version
- No code required for basic dashboards
- Connects directly to PostgreSQL

**Why a BI tool instead of custom code:**
- Business users can create their own reports
- Drag-and-drop interface saves engineering time
- Standard charts and visualizations built-in
- Automatic refresh capabilities

**Real-world parallel:** Enterprises use Tableau, Looker, or Power BI. Metabase provides similar functionality for learning and small teams.

---

## Recent Improvements (v2)

This project was refactored to reflect production engineering practices. Here's what changed and why.

### Pandera: Schema Validation

**Before:** Data was validated manually with `if/else` checks scattered across the pipeline — fragile, inconsistent, and hard to extend.

**After:** [Pandera](https://pandera.readthedocs.io/) schemas define the expected shape of data as Python classes:

```python
class CleanedSalesSchema(pa.DataFrameModel):
    order_id: Series[str] = pa.Field(str_matches=r"^ORD-\d{6}$")
    quantity: Series[int] = pa.Field(ge=1, le=10000)
    unit_price: Series[float] = pa.Field(gt=0, le=100000)
```

Three schemas enforce data contracts at each stage of the pipeline:

| Schema | Stage | Validates |
|--------|-------|-----------|
| `RawSalesSchema` | After file read | Column presence, parseable types |
| `CleanedSalesSchema` | After cleaning | Data types, value ranges, pattern matching |
| `EnrichedSalesSchema` | After enrichment | Calculated fields exist and are correct |

**Why this matters:** If the upstream data format changes (e.g., `quantity` starts arriving as a float), Pandera catches it immediately with a clear error message instead of silently corrupting downstream data.

---

### File-Based Logging

**Before:** All logs went to stdout/stderr — useful during development but lost after container restart and impossible to search historically.

**After:** `utils/logging_config.py` routes all pipeline logs to persistent files:

```
airflow/logs/pipeline/sales_pipeline.log          # All levels
airflow/logs/pipeline/sales_pipeline_errors.log   # Errors only
```

Logs rotate daily with 5 backup files kept. Each entry captures:
- Timestamp
- Logger name (e.g., `minio_service`, `data_cleaner`)
- Log level
- Message

```python
# Usage anywhere in the pipeline
from utils.logging_config import get_logger
log = get_logger("my_module")
log.info("Processing file: sales_2026-02-27.csv")
```

**Why this matters:** When a pipeline fails at 3am, you need the log from that exact run — not just whatever is still in Docker's memory buffer.

---

### Separation of Concerns: Modular Package Structure

**Before:** All logic — validation, connections, MinIO calls, transformations — lived in one 350-line `sales_pipeline.py` file. Any change risked breaking everything.

**After:** Responsibilities are split into four focused packages:

```
airflow/dags/
├── utils/               # Cross-cutting concerns
│   ├── logging_config.py    # One place to configure logging
│   └── connections.py       # One place to manage credentials
├── validation/          # Data contracts
│   ├── schemas.py           # Pandera schema definitions
│   └── validators.py        # Business rule validation
├── services/            # External system clients
│   ├── minio_service.py     # All MinIO operations
│   └── postgres_service.py  # All PostgreSQL operations
└── transformers/        # Data transformation logic
    ├── data_cleaner.py      # Cleaning rules
    └── data_enricher.py     # Enrichment and calculated fields
```

Each module follows the **single responsibility principle** — it does one thing and does it completely.

| Module | Owns | Does NOT touch |
|--------|------|----------------|
| `minio_service.py` | File download/upload/archive | Data transformation |
| `data_cleaner.py` | Deduplication, type casting, nulls | Database calls |
| `postgres_service.py` | Upsert logic, health checks | Business rules |
| `validators.py` | Validation reporting | Cleaning logic |
| `sales_pipeline.py` | Task wiring only | Implementation details |

**Why this matters:** When the MinIO bucket path changes, you edit one file. When the cleaning logic needs a new rule, you edit one file. When a junior engineer joins, they can find everything relevant to their task in one place.

---

### Comprehensive Test Suite

**Before:** No tests. Any change could silently break the pipeline.

**After:** 8 test files covering every layer:

```
tests/
├── conftest.py              # Shared fixtures (sample DataFrames, mocks)
├── test_schemas.py          # Pandera schema validation tests
├── test_validators.py       # Business rule validation tests
├── test_data_cleaner.py     # Cleaning logic tests
├── test_data_enricher.py    # Enrichment logic tests
├── test_minio_service.py    # MinIO operations (mocked)
├── test_postgres_service.py # PostgreSQL operations (mocked)
└── test_integration.py      # End-to-end pipeline tests
```

Run the suite with:

```bash
pip install -r tests/requirements.txt
pytest tests/ -v --cov=airflow/dags
```

External dependencies (MinIO, PostgreSQL) are mocked so tests run without Docker. Integration tests verify the full data flow from raw CSV to enriched output.

**Why this matters:** You can refactor the cleaning logic and know immediately — before pushing to production — whether anything broke.

---

## Why Separation of Concerns Matters

Notice that each component has ONE job:

| Component | Responsibility | Does NOT do |
|-----------|---------------|-------------|
| MinIO | Store raw files | Process data |
| Airflow | Orchestrate tasks | Store data long-term |
| PostgreSQL | Store clean data | Visualize data |
| Metabase | Visualize data | Process data |

The same principle applies inside the code itself — each Python module owns exactly one concern. See the package structure above.

**Why this matters:**

1. **Independent scaling:** If dashboards are slow, scale Metabase without touching the pipeline
2. **Technology flexibility:** Replace PostgreSQL with Snowflake without rewriting the pipeline
3. **Team boundaries:** Different engineers can own different components
4. **Failure isolation:** If Metabase crashes, data still flows into PostgreSQL
5. **Testing:** Each component can be tested independently

This is called "microservices architecture" at the infrastructure level and "separation of concerns" at the code level. Both are how modern data platforms are built.

---

## How Data Flows Step-by-Step

Let's trace a single CSV file through the entire system:

### Step 1: Data Generation
```
User runs: python generate_sales.py --records 100 --upload
```

The generator creates a CSV file with realistic (but imperfect) sales data and uploads it to MinIO's `raw-sales` bucket.

### Step 2: File Detection
```
Airflow task: detect_new_files
```

Every 15 minutes, Airflow wakes up and checks the `raw-sales` bucket for CSV files. It passes the list of files to the next task via XCom.

### Step 3: Schema Validation
```
Airflow task: validate_schema
```

For each file, `MinIOService` downloads the CSV and `DataValidator` applies the **Pandera `RawSalesSchema`**, checking:
- All required columns are present
- Column types are parseable
- `order_id` matches the `ORD-XXXXXX` pattern
- Countries are from the known valid set

Invalid files are logged to `logs/pipeline/sales_pipeline_errors.log` and skipped. Valid files continue processing.

### Step 4: Data Cleaning
```
Airflow task: clean_data
```

`DataCleaner` applies cleaning rules, then `DataValidator` re-validates the result against **`CleanedSalesSchema`**:
- Remove duplicate order IDs
- Parse and validate dates
- Convert quantities and prices to numbers
- Remove rows with invalid values
- Fill missing values where appropriate
- Trim whitespace from strings

A `CleaningResult` dataclass is returned with stats (rows removed, reasons). All progress is written to the pipeline log file.

### Step 5: Compute Derived Fields
```
Airflow task: compute_totals
```

`DataEnricher` calculates derived fields, then `DataValidator` validates against **`EnrichedSalesSchema`**:
- `total_amount = quantity * unit_price`
- `ingestion_timestamp` (when the record was processed)
- `revenue_category` (`low`, `medium`, `high`) based on total_amount

Datetime columns are converted to ISO strings before being passed to the next task via XCom (required for JSON serialisation).

### Step 6: Database Load
```
Airflow task: load_to_postgres
```

`PostgresService` runs a health check, then inserts clean data using an "upsert" pattern:
- New order IDs are inserted
- Existing order IDs are updated
- This handles reprocessing gracefully

A `LoadResult` dataclass captures rows inserted, rows updated, and duration.

### Step 7: Archival
```
Airflow task: archive_processed_files
```

Successfully processed files are:
1. Copied to the `archive` bucket with a date-based path
2. Deleted from `raw-sales`

This prevents reprocessing and provides an audit trail.

### Step 8: Visualization
```
User accesses: http://localhost:3000 (Metabase)
```

Metabase queries PostgreSQL to render:
- Total sales over time
- Sales by country
- Top products
- Average order value

---

## How This Mimics Real-World Platforms

### Same Patterns, Different Scale

| This Project | Production Equivalent |
|--------------|----------------------|
| MinIO bucket | Amazon S3 / Azure Blob |
| Airflow on Docker | Airflow on Kubernetes / MWAA |
| PostgreSQL | Snowflake / BigQuery / Redshift |
| Metabase | Tableau / Looker / Power BI |
| CSV files | API events / log files / database CDC |
| Pandera schemas | Great Expectations / dbt schema tests |
| Rotating file logs | CloudWatch / Datadog / Splunk |
| pytest test suite | CI-gated test pipelines (GitHub Actions) |
| Modular DAG packages | Shared internal Python libraries |

### Same Problems, Same Solutions

**Problem: Data arrives with quality issues**
- Our solution: Pandera schemas (`RawSalesSchema`, `CleanedSalesSchema`, `EnrichedSalesSchema`) enforce contracts at every stage
- Production: Great Expectations, dbt tests, data contracts

**Problem: Need to track what processed when**
- Our solution: Airflow task logs and archival
- Production: Data lineage tools (OpenLineage, Marquez)

**Problem: Pipeline might fail mid-process**
- Our solution: Airflow retries and idempotent loads
- Production: Same pattern, plus alerting (PagerDuty, Slack)

**Problem: Business needs vary**
- Our solution: Metabase for self-service
- Production: Semantic layer, governed metrics

### What Production Adds

When scaling to production, you'd add:

1. **Authentication:** SSO, role-based access
2. **Secrets management:** HashiCorp Vault, AWS Secrets Manager
3. **Monitoring:** Prometheus, Grafana, DataDog
4. **Alerting:** PagerDuty, Slack notifications
5. **Data quality:** Great Expectations, dbt tests
6. **Version control:** Git-based DAG deployment
7. **Infrastructure as code:** Terraform, Pulumi

But the fundamental architecture remains the same.

---

## Key Takeaways

1. **Tools are chosen for specific jobs.** Don't use a database for file storage or a file system for queries.

2. **Separation enables change.** You can swap components without rewriting everything — both at the infrastructure level (MinIO → S3) and the code level (each module owns one concern).

3. **Orchestration is critical.** Without Airflow, you'd have disconnected scripts with no visibility.

4. **Data quality is not optional.** Pandera schemas catch bad data at the boundary before it corrupts downstream systems. Bad data in means bad decisions out.

5. **Log to files, not console.** Container stdout is ephemeral. File logs survive restarts and are searchable after the fact.

6. **Test every layer.** Unit tests for schemas, validators, and transformers. Integration tests for the full flow. Mock external dependencies so tests run anywhere.

7. **Archive everything.** You never know when you'll need to reprocess.

8. **This scales up.** The same architecture works from 100 rows to 100 billion rows.

---

## Questions to Reflect On

As you work with this project, consider:

1. What would break if MinIO went down?
2. How would you add a new data source?
3. What if business users needed hourly updates instead of daily?
4. How would you handle a file with 10 million rows?
5. What if the CSV format changed?

These are the kinds of questions you'll face in real data engineering work.
