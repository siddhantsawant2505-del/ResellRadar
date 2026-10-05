# ResellRadar

ResellRadar is a full-stack resale marketplace analytics project that combines synthetic listing generation, raw data ingestion, HDFS persistence, Apache Spark processing, and a telemetry dashboard for monitoring the pipeline.

The repository currently models a realistic end-to-end data engineering pipeline for second-hand mobile phones and furniture listings, including:

- synthetic batch generation at scale
- raw schema validation
- HDFS upload orchestration
- Spark-based cleaning and entity resolution
- curated analytics tables for price and resale trends
- a live monitoring dashboard for ingest jobs and pipeline telemetry

---

## Project goal

The project is designed to simulate a production-style data pipeline for resale market intelligence:

- generate large volumes of realistic listing data
- normalize it into a shared schema
- resolve duplicate or near-duplicate inventory records
- detect repost patterns
- compute depreciation, resale velocity, and regional variance metrics
- expose the pipeline through a single monitoring interface

---

## Architecture overview

```text
Synthetic dataset / scraper input
        ↓
raw JSON batches in data/raw
        ↓
validation + schema checks
        ↓
HDFS sync / raw lake storage
        ↓
PySpark cleaning + normalization
        ↓
entity resolution + repost detection
        ↓
feature engineering / curated parquet tables
        ↓
dashboard telemetry + raw preview UI
```

This repository is not a single script; it is a small data platform with several moving parts across Python, Spark, HDFS, Docker, and Next.js.

---

## Key components

### 1. Data generation and raw ingestion

- `scraper/generator.py`: lightweight synthetic listing generator used by the monitoring API to create raw batches in `data/raw`.
- `generator/generate_data.py`: larger-scale synthetic data generator that creates partitioned runs in `data/raw/generated/` and ground-truth outputs in `data/ground_truth/`.
- `scraper/validator.py`: validates raw-zone listings against the project schema and data-quality rules.
- `scraper/jsonl_converter.py`: converts JSON batches into JSONL for downstream processing.
- `server.py`: FastAPI service that exposes job-control, status, logs, HDFS sync, and preview endpoints.
- `hdfs_uploader.py`: pushes un-synced raw files into HDFS or a pseudo-distributed local equivalent.

### 2. Spark processing pipeline

- `spark_jobs/clean_normalize.py`: standardizes raw listing fields, cleans text, removes incomplete rows, and writes parquet outputs.
- `spark_jobs/entity_resolution.py`: resolves related listings and flags likely reposts using normalized-title matching and entity IDs.
- `spark_jobs/feature_engineering.py`: builds curated analytics tables such as depreciation curves and price variance summaries.
- `scripts/accept_pipeline.py`: validates stage outputs against expected counts and schema expectations.

### 3. Dashboard and monitoring

- `dashboard/`: Next.js app that polls the FastAPI server for job status, live logs, HDFS status, and raw preview data.
- `dashboard/components/`: UI panels for metrics, job triggers, log streams, HDFS status, and the data grid.

### 4. Infrastructure and docs

- `docker-compose.yml`: local single-node HDFS stack.
- `docker/`: Hadoop configuration files.
- `docs/`: setup and project documentation.
- `logs/`: acceptance and validation evidence artifacts.

---

## Repository layout

```text
ResellRadar/
├── dashboard/                    # Next.js monitoring UI
│   ├── app/
│   ├── components/
│   ├── package.json
│   └── ...
├── data/
│   ├── raw/                     # raw JSON listing batches
│   ├── processed/               # Spark stage outputs
│   ├── curated/                 # analytics tables
│   ├── ground_truth/            # generated truth data for evaluation
│   └── hdfs_sync_manifest.json
├── docker/                      # Hadoop / HDFS container config
├── docs/                        # setup and process docs
├── generator/
│   ├── calibration.py
│   └── generate_data.py         # large synthetic data generation job
├── logs/                        # validation and acceptance outputs
├── scraper/
│   ├── __init__.py
│   ├── config.py
│   ├── generator.py             # lightweight listing generator
│   ├── jsonl_converter.py
│   ├── SCHEMA.md
│   ├── spider.py
│   ├── validator.py
│   └── ...
├── scripts/
│   ├── accept_pipeline.py
│   ├── evaluate_er.py
│   ├── learn_chatter_vocab.py
│   ├── push_to_hdfs.py
│   ├── qa_generated.py
│   └── ...
├── spark_jobs/
│   ├── clean_normalize.py
│   ├── entity_resolution.py
│   ├── entity_resolution_v2.py
│   ├── entity_resolution_v3.py
│   ├── feature_engineering.py
│   └── ...
├── .gitignore
├── docker-compose.yml
├── hdfs_uploader.py
├── requirements.txt
├── server.py
├── README.md
└── ...
```

---

## Typical data flow

### Raw generation

```bash
python -c "from scraper.generator import generate_batch; generate_batch(count=500, category='all')"
```

This produces raw JSON batches under `data/raw/` using the simulated marketplace generator.

### Large synthetic generation run

```bash
python generator/generate_data.py --rows 10000
```

This is the project’s higher-volume synthetic-generation path and writes run-partitioned output under `data/raw/generated/` plus ground-truth files under `data/ground_truth/`.

### Validation and schema checks

```bash
python -m scraper.validator
```

This checks the raw dataset against the contract defined in `scraper/SCHEMA.md` and exits non-zero if the data does not meet expected quality rules.

### Prepare the Spark handoff

```bash
python -m scraper.jsonl_converter
```

This converts the raw JSON batches to a JSONL handoff format for the Spark pipeline.

### Spark stages

```bash
python spark_jobs/clean_normalize.py
python spark_jobs/entity_resolution.py
python spark_jobs/feature_engineering.py
```

Expected outputs include:

- `data/processed/clean_listings.parquet`
- `data/processed/entity_resolved.parquet`
- `data/curated/depreciation_curve_curated.parquet`
- `data/curated/resale_velocity_curated.parquet`
- `data/curated/regional_price_variance_curated.parquet`

### Acceptance checks

```bash
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
```

This verifies the stage outputs against expected counts, schema expectations, and pipeline contracts.

---

## HDFS setup

The project includes a Docker-based Hadoop single-node setup for local experiments:

```bash
docker compose -f docker-compose.yml up -d
```

This starts the NameNode/DataNode stack for local HDFS access. Detailed setup guidance is in `docs/HDFS_SETUP.md` and `docs/SPARK_SETUP.md`.

To push raw files into HDFS:

```bash
python hdfs_uploader.py
```

or the project-specific push helper if present in the scripts folder.

---

## Dashboard and control server

The Python backend exposes state and control endpoints for the dashboard:

```bash
python server.py
```

The server listens by default on `http://127.0.0.1:8000` and provides routes for:

- telemetry status
- simulate scrape jobs
- stop jobs
- HDFS sync
- logs
- raw data preview

Start the dashboard separately:

```bash
cd dashboard
npm install
npm run dev
```

Then open the UI at:

- `http://localhost:3000`

---

## Environment setup

### Python dependencies

```bash
python -m venv venv
# Windows PowerShell
.\venv\Scripts\Activate.ps1
# Linux / macOS
source venv/bin/activate
pip install -r requirements.txt
```

### Frontend dependencies

```bash
cd dashboard
npm install
```

---

## Recommended local workflow

For a full local demo, the typical sequence is:

```bash
python generator/generate_data.py --rows 10000
python -m scraper.validator
python -m scraper.jsonl_converter
python spark_jobs/clean_normalize.py
python spark_jobs/entity_resolution.py
python spark_jobs/feature_engineering.py
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python server.py
cd dashboard && npm run dev
```

This reflects the actual code in the current repository and is the best starting point for running the project end-to-end.

---

## Notes

- Raw files can be large and are not always committed to the repo.
- HDFS and Spark setup depend on local environment configuration and Java/Hadoop availability.
- The project is intentionally organized as a mini data platform, not a single app, so some commands must be run in different terminals or environment contexts.

For environment-specific steps and deeper operational notes, see:

- `docs/HDFS_SETUP.md`
- `docs/SPARK_SETUP.md`
- `README_PERSON2.md`
- `DESIGN.md`
