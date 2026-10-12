# Spark Big Data Processing Setup

## 1. Environment Requirements

The Spark stages are executed **inside a Docker container**, not on the Windows host: the
job files address `hdfs://namenode:9000`, and only a process on the compose network can
resolve that hostname, transfer blocks, and write parquet without Windows `winutils`.

| Component | Where it runs / version |
| --------- | ----------------------- |
| Spark | 4.2.0 — image `resellradar/spark:4.2.0-numpy` (stock `apache/spark:4.2.0` + numpy), built from `docker/spark/Dockerfile` |
| HDFS | `docker compose` NameNode + DataNode (`apache/hadoop:3`), `hdfs://namenode:9000` |
| Spark Standalone cluster (optional) | compose `cluster` profile: `spark-master` + scalable `spark-worker` units |
| Python for the host-side scripts | 3.12 (generator, validation, acceptance, ER grading; `pyspark==4.2.0` is in `requirements.txt` for parity) |

## 2. Start the storage (and optionally the cluster)

```powershell
# HDFS only (single-unit Spark execution)
docker compose -f docker-compose.yml up -d

# HDFS + Spark Standalone master and 3 worker units (distributed batch mode)
docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3
```

The Spark image is built automatically on first harness use (or explicitly with
`docker compose build spark-master`).

## 3. The execution harness

`scripts/run_stage_in_docker.sh` runs an **unmodified** job file inside the Spark image,
bind-mounting the repo at `/work` and joining the HDFS network:

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
```

- Default mode: one driver JVM using `local[*]` (all host cores).
- `CLUSTER=1 …`: submits the same job to the Standalone cluster, with executors on the
  worker units reading/writing the HDFS zones (`RR_DATA_ROOT=hdfs://namenode:9000/data`).
- Knobs: `DRIVER_MEMORY`, `EXECUTOR_MEMORY`, `EXECUTOR_CORES`, `SPARK_MASTER_URL`,
  `RR_DATA_ROOT`, `SPARK_IMAGE`, `DETACH=1`, `CONTAINER_NAME=…`.

See `docs/CLUSTER_BATCH_MODE.md` for the full distributed-batch guide.

## 4. Input dataset

The Spark jobs read the raw zones (HDFS by default, or the local `data/` checkout when
`RR_DATA_ROOT` is unset):

```text
/data/raw/mercari/train.tsv                          # real, 1,482,535 rows
/data/raw/generated/run=2026_09_28T150245Z/*.jsonl   # synthetic, 500,000 rows (canonical run)
/data/raw/uploads/run=<ts>/*.jsonl                   # optional operator uploads (third source)
```

Stage 1 unions all three sources into the 19-field schema
(`docs/SCHEMA_MISMATCHES.md`); the canonical generated run is pinned inside
`spark_jobs/clean_normalize.py`, and `docs/SCHEMA_uploads.md` covers the upload zone.

## 5. Run the Big Data Processing Pipeline

Run the following from the project root (HDFS stack up).

### Step 0 — learn the ER chatter vocabulary (required before Stage 2)

```bash
python scripts/learn_chatter_vocab.py
```

### Step 1 — Cleaning and Normalization

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
```

Output:

```text
data/processed/clean_listings.parquet    (1,972,679 rows, 21 columns)
```

This step performs:

* Schema normalization (19-field union across the three sources)
* Text cleaning (`title_clean`, `description_clean`)
* Timestamp conversion and null handling
* Duplicate listing removal (`dropDuplicates` on `listing_id`)
* Parquet conversion

---

### Step 2 — Entity Resolution

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
```

Output:

```text
data/processed/entity_resolved.parquet   (1,972,679 rows, 24 columns)
```

This step performs:

* Title tokenization + whitespace normalization
* Chatter removal using the learned vocabulary (`data/processed/chatter_vocab.json`)
* Entity keying on the sorted product-token set (storage/colour/model kept)
* Deterministic entity ID assignment
* Repost detection + repost → original linking (1–14 day window)

The accepted run produced:

```text
1,972,679 listings
1,040,681 unique entities
49,788 reposts
Task A same-product F1 0.9802 (P 1.0000 / R 0.9611)
```

---

### Step 3 — Feature Engineering

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
```

This generates the curated analytics datasets:

```text
data/curated/depreciation_curve_curated.parquet          (1,062,905 rows)
data/curated/resale_velocity_curated.parquet             (1,105 rows)
data/curated/regional_price_variance_curated.parquet     (1,056,069 rows)
```

The generated analytics include:

* Per-entity depreciation curves (price drift vs the entity's launch-month price)
* Resale velocity (days from posted to delisted)
* Regional price variance

---

## 6. Acceptance / grading

```bash
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python scripts/evaluate_er.py --pred data/processed/entity_resolved.parquet
```

`accept_pipeline.py` derives its expected row counts from the raw zone at runtime (so an
uploaded batch or a rebuild shifts the expectations instead of failing the gate);
`evaluate_er.py` grades entity resolution against the generator's ground truth.

## 7. Processing Flow

```text
Raw zones (Mercari + generated + uploads)
     ↓
Cleaning & Normalization
     ↓
clean_listings.parquet
     ↓
Entity Resolution (+ learned chatter vocabulary)
     ↓
entity_resolved.parquet
     ↓
Feature Engineering
     ↓
Curated Analytics Tables
     ↓
Acceptance checks + dashboard/analytics consumers
```

## 8. Important Git/Data Note

Generated datasets are intentionally excluded from Git:

```text
data/raw/
data/processed/
data/curated/
```

The repository stores the **processing code**; the datasets are rebuildable artifacts that
live in the local checkout and on HDFS. Do not commit large raw JSON/TSV files or generated
Parquet directories to GitHub.

## 9. Recommended Run Order for Integration

After pulling `main`:

```bash
docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3
python scripts/learn_chatter_vocab.py
bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
bash scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
bash scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
```

Or click **Start pipeline** on the dashboard (`http://localhost:3000`) to run the whole
sequence, distributed automatically when the cluster profile is up.

After successful execution the curated Parquet datasets can be consumed by the project's
downstream analytics/dashboard components (the Next.js analytics page reads them through
the FastAPI `/api/analytics` endpoint).
