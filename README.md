# ResellRadar

ResellRadar is a full-stack resale marketplace analytics project that combines real marketplace data ingestion, synthetic listing generation, raw data validation, HDFS persistence, distributed Apache Spark processing, and a telemetry dashboard for monitoring and analyzing the pipeline.

The repository currently models a realistic end-to-end data engineering pipeline for second-hand marketplace listings (real Mercari data plus a calibrated synthetic source), including:

- dual-source raw data ingestion (real Mercari listings + synthetic generator)
- raw schema validation and generator QA
- HDFS upload orchestration (raw / processed / curated zones)
- distributed Spark processing on a Dockerized Spark Standalone cluster (with single-unit fallback)
- entity resolution and repost detection
- curated analytics tables for price and resale trends
- a live monitoring dashboard for ingest jobs, pipeline telemetry, and the batch cluster
- a separate analytics page with KPIs, charts, and auto-generated insights

---

## Project goal

The project is designed to simulate a production-style data pipeline for resale market intelligence:

- ingest real marketplace listings and generate large volumes of realistic synthetic data
- normalize both sources into a shared schema
- resolve duplicate or near-duplicate inventory records into product entities
- detect repost patterns
- compute depreciation (launch-cohort price drift), resale velocity, and regional variance metrics
- expose the pipeline through a single monitoring interface, with a dedicated analytics view

---

## Architecture overview

```text
Real Mercari listings + synthetic generator (generator/generate_data.py)
        ↓
raw JSON batches in data/raw
        ↓
validation + schema checks (scripts/validate_raw.py)
        ↓
HDFS sync / raw zone storage (scripts/push_to_hdfs.py)
        ↓
PySpark cleaning + normalization        ← Spark Standalone cluster
        ↓                                   (spark-master + 3 worker
entity resolution + repost detection         containers; falls back to
        ↓                                    host local[*] when down)
feature engineering / curated parquet tables
        ↓
HDFS processed + curated zones (synced back for acceptance)
        ↓
acceptance checks (scripts/accept_pipeline.py)
        ↓
dashboard telemetry + raw preview UI  |  /analytics page (KPIs, charts, insights)
```

This repository is not a single script; it is a small data platform with several moving parts across Python, Spark, HDFS, Docker, and Next.js.

---

## Key components

### 1. Data generation and raw ingestion

- `scripts/download_data.py`: fetches the real Mercari source data with checksums.
- `scripts/extract_seed_sample.py`: calibration sample used to tune the synthetic generator.
- `generator/generate_data.py`: large-scale synthetic data generator that creates partitioned runs in `data/raw/generated/` and ground-truth outputs in `data/ground_truth/`.
- `scripts/validate_raw.py`: dual-source raw-zone profiling and structural validation; outputs `logs/validation_report.md`.
- `scripts/qa_generated.py`: 27-check QA suite for the generator, including truth-file integrity.
- `scripts/push_to_hdfs.py`: pushes un-synced raw files into the HDFS raw zone and maintains `data/hdfs_sync_manifest.json`.
- `scripts/push_zones_to_hdfs.py`: lands the processed/curated parquet zones on HDFS.
- `scripts/ingest_uploads.py`: manual ingestion — normalizes an uploaded CSV/TSV/JSON/JSONL into the 19-field union schema and stores it as `data/raw/uploads/run=<ts>/*.jsonl` (title + price required per row; existing `listing_id`s are skipped so re-uploads never duplicate).
- `server.py`: FastAPI service exposing job control, status, logs, HDFS sync, cluster telemetry, analytics aggregates, and preview endpoints.

### 2. Spark processing pipeline

- `spark_jobs/clean_normalize.py`: standardizes raw listing fields, cleans text, removes incomplete rows, and writes parquet outputs.
- `spark_jobs/entity_resolution.py`: resolves related listings into product entities and flags likely reposts (v3 bounded LSH approach; see `docs/FINAL_REPORT.md`).
- `spark_jobs/feature_engineering.py`: builds curated analytics tables such as the launch-cohort price-drift curve, resale velocity, and regional price variance.
- `scripts/run_stage_in_docker.sh`: execution harness for the Spark stages — single-unit mode by default, `CLUSTER=1` for distributed runs on the Spark Standalone cluster.
- `scripts/accept_pipeline.py`: validates stage outputs against expected counts and schema expectations.
- `scripts/evaluate_er.py`: pairwise precision/recall/F1 grading of entity resolution against ground truth.
- `scripts/learn_chatter_vocab.py`: learns the chatter vocabulary used by entity resolution (run before the Spark stages).
- `scripts/verify_hdfs_zones.py`: distributed read-back proof of all HDFS tables.

The Spark jobs read `SPARK_MASTER` (falling back to `local[*]`) and `RR_DATA_ROOT` (falling back to local `data/`), so the same job files run single-unit or distributed unchanged — see `docs/CLUSTER_BATCH_MODE.md`. Manual uploads union into `clean_normalize.py` as a third source (`source_platform="uploads"`), and acceptance derives its expected counts from the raw zone at runtime, so an uploaded batch shifts the expectations instead of failing the gate.

### 3. Dashboard and monitoring

- `dashboard/`: Next.js app that polls the FastAPI server through same-origin proxy routes (`app/api/*/route.ts`).
- Ingestion console (`http://localhost:3000`): live metrics, job trigger panel, log terminal, Spark cluster panel (`dashboard/components/ClusterPanel.tsx` — worker units with live CPU/RAM, running and finished applications), manual upload panel (`dashboard/components/UploadPanel.tsx`), HDFS sync panel, and raw data grid.
- Analytics page (`http://localhost:3000/analytics`): KPI cards, charts (price drift, category prices, price bands, brands, repost rate by category, cluster sizes, resale speed, regional volume), and an insights section — computed by the backend from the processed parquet tables (`dashboard/components/SimpleCharts.tsx`, dependency-free SVG charts).

### 4. Infrastructure and docs

- `docker-compose.yml`: local Hadoop stack (NameNode + DataNode) plus the optional `cluster` profile: a Spark Standalone master and scalable `spark-worker` containers.
- `docker/hadoop/`: Hadoop configuration files.
- `docker/spark/`: Dockerfile for the Spark client image (`resellradar/spark:4.2.0-numpy`).
- `docs/`: setup and project documentation (see the links at the bottom).
- `logs/`: acceptance and validation evidence artifacts.

---

## Verified results (canonical run)

Raw 1,982,535 → clean **1,972,679** rows (Mercari 1,482,535 + generated 490,144), **1,040,681** product entities, **49,788** reposts, curated tables **1,062,905 / 1,105 / 1,056,069** rows; acceptance checks 24/24 PASS (`logs/acceptance_checks.md`). Identical counts are reproduced in distributed mode — see `docs/CLUSTER_BATCH_MODE.md`.

---

## Repository layout

```text
ResellRadar/
├── dashboard/                    # Next.js UI (ingestion console + analytics page)
│   ├── app/                      #   pages + /api proxy routes to server.py
│   ├── components/               #   panels, cluster panel, analytics charts
│   └── package.json
├── data/
│   ├── raw/                      # raw JSON listing batches (real + synthetic + manual uploads)
│   │   └── uploads/run=<ts>/     # manually uploaded batches (JSONL + summary)
│   ├── processed/                # Spark stage outputs (parquet)
│   ├── curated/                  # analytics tables (parquet)
│   ├── ground_truth/             # generated truth data for evaluation
│   └── hdfs_sync_manifest.json
├── docker/
│   ├── hadoop/                   # Hadoop / HDFS container config
│   └── spark/                    # Spark client image Dockerfile
├── docs/                         # setup and process docs
├── generator/
│   ├── calibration.py
│   └── generate_data.py          # large synthetic data generation job
├── logs/                         # validation and acceptance outputs
├── scripts/
│   ├── accept_pipeline.py        # structural acceptance checks
│   ├── download_data.py          # real source data download
│   ├── evaluate_er.py            # ER precision/recall/F1 grading
│   ├── extract_seed_sample.py    # generator calibration input
│   ├── ingest_uploads.py         # manual upload normalization + validation
│   ├── learn_chatter_vocab.py    # ER chatter vocabulary learner
│   ├── push_to_hdfs.py           # raw zone push + manifest
│   ├── push_zones_to_hdfs.py     # processed/curated zone push
│   ├── qa_generated.py           # generator QA suite
│   ├── run_stage_in_docker.sh    # Spark stage harness (single-unit / CLUSTER=1)
│   ├── validate_raw.py           # raw-zone validation gate
│   └── verify_hdfs_zones.py      # distributed HDFS read-back proof
├── spark_jobs/
│   ├── clean_normalize.py
│   ├── entity_resolution.py
│   └── feature_engineering.py
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── server.py
└── README.md
```

---

## Typical data flow

### Raw generation

```bash
python scripts/download_data.py                  # real source data (once)
python scripts/extract_seed_sample.py            # generator calibration input
python generator/generate_data.py --rows 10000   # synthetic run
```

This writes run-partitioned synthetic output under `data/raw/generated/` plus ground-truth files under `data/ground_truth/`, alongside the real Mercari batches.

### Validation and QA gates

```bash
python scripts/validate_raw.py    # dual-source profiling -> logs/validation_report.md
python scripts/qa_generated.py    # generator QA (27 checks)
```

### HDFS raw zone push

```bash
python scripts/push_to_hdfs.py    # requires the HDFS stack to be up
```

### Spark stages

Either run them directly (single-unit):

```bash
python scripts/learn_chatter_vocab.py     # learn ER vocabulary first
python spark_jobs/clean_normalize.py
python spark_jobs/entity_resolution.py
python spark_jobs/feature_engineering.py
```

or distributed on the Spark Standalone cluster (see the next section):

```bash
CLUSTER=1 bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
CLUSTER=1 bash scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
CLUSTER=1 bash scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
```

Expected outputs include:

- `data/processed/clean_listings.parquet`
- `data/processed/entity_resolved.parquet`
- `data/curated/depreciation_curve_curated.parquet`
- `data/curated/resale_velocity_curated.parquet`
- `data/curated/regional_price_variance_curated.parquet`

### Acceptance and grading

```bash
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python scripts/evaluate_er.py --pred data/processed/entity_resolved.parquet
```

### One-click path

Alternatively, click **Start pipeline** on the dashboard (`http://localhost:3000`): it runs generation → validation → HDFS push → Spark stages (distributed automatically when the cluster profile is up, with outputs synced back from HDFS before acceptance) → acceptance checks, streaming progress into the log terminal and the cluster panel.

### Manual upload path

To add your own data, use the **Manual ingestion** panel on the console: pick a CSV/TSV/JSON/JSONL file (each row needs at least `title` and `price`; a header alias map handles `name`/`item_description`/`category_name`-style columns), optionally label the batch, and hit **Ingest & merge**. The file is normalized into `data/raw/uploads/run=<ts>/`, pushed to the HDFS raw zone, and unioned into the Spark stages on the rerun — the acceptance gate derives its expected counts from the raw zone, so merged runs stay fully verified. Uploads never duplicate: rows whose `listing_id` already exists in the raw zone are skipped.

---

## HDFS and Spark cluster setup

The project includes a Docker-based Hadoop single-node setup for local experiments, plus an optional distributed Spark cluster:

```bash
# storage stack (NameNode + DataNode)
docker compose -f docker-compose.yml up -d

# storage + Spark Standalone cluster (master + 3 worker units)
docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3
```

The Spark Master web UI is at `http://localhost:8080`. Sizing, submission details, and the distributed-run proof are documented in `docs/CLUSTER_BATCH_MODE.md`.

To push raw files into HDFS:

```bash
python scripts/push_to_hdfs.py
```

Detailed storage guidance is in `docs/HDFS_SETUP.md` and `docs/SPARK_SETUP.md`.

---

## Dashboard and control server

The Python backend exposes state and control endpoints for the dashboard:

```bash
python server.py
```

The server listens by default on `http://127.0.0.1:8000` and provides routes for:

- telemetry status (`/api/status`)
- pipeline trigger / stop (`/api/scrape/trigger`, `/api/scrape/stop`; trigger accepts `skip_generation: true` for rebuild-from-raw-zone runs)
- manual ingestion (`/api/ingest/upload`, `/api/ingest/uploads` — upload a CSV/TSV/JSON/JSONL batch and optionally rerun the pipeline to merge it)
- HDFS sync (`/api/hdfs/sync`)
- logs and raw data preview (`/api/logs`, `/api/preview`)
- Spark cluster telemetry (`/api/cluster` — worker units, running and finished applications)
- analytics aggregates (`/api/analytics` — KPIs, chart series, insights)

Start the dashboard separately:

```bash
cd dashboard
npm install
npm run dev
```

Then open:

- `http://localhost:3000` — ingestion console
- `http://localhost:3000/analytics` — analytics page (KPIs, charts, insights)

---

## Environment setup

### Python dependencies

```bash
pip install -r requirements.txt
```

(Optionally inside a virtual environment; the project currently runs on the system Python 3.12 installation.)

### Frontend dependencies

```bash
cd dashboard
npm install
```

---

## Recommended local workflow

For a full local demo, the typical sequence is:

```bash
docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3
python scripts/download_data.py
python scripts/extract_seed_sample.py
python generator/generate_data.py --rows 10000
python scripts/validate_raw.py
python scripts/push_to_hdfs.py
python scripts/learn_chatter_vocab.py
python spark_jobs/clean_normalize.py
python spark_jobs/entity_resolution.py
python spark_jobs/feature_engineering.py
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python server.py
cd dashboard && npm run dev
```

This reflects the actual code in the current repository and is the best starting point for running the project end-to-end. For a hands-off run, use the dashboard's **Start pipeline** button instead of the middle commands.

---

## Notes

- Raw files can be large and are not always committed to the repo.
- HDFS and Spark setup depend on local environment configuration and Docker Desktop resources; the cluster sizing fits a 16-CPU / ~7.6 GB Docker VM.
- Dates, cities, and seller ids exist only for the synthetic source (~25% of rows); the real Mercari rows carry no such fields. The analytics page labels the charts that describe that slice only.
- The project is intentionally organized as a mini data platform, not a single app, so some commands must be run in different terminals or environment contexts.

For environment-specific steps and deeper operational notes, see:

- `docs/HDFS_SETUP.md`
- `docs/SPARK_SETUP.md`
- `docs/CLUSTER_BATCH_MODE.md` — distributed batch processing and the dashboard trigger integration
- `docs/FINAL_REPORT.md` — results, evidence index, and reproduction steps
- `README_PERSON2.md`
- `DESIGN.md`
