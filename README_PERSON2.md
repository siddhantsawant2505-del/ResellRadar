# ResellRadar — Person 2: Big Data Processing

## Role

**Person 2 — Big Data Processing Engineer**

Responsible for transforming the raw ResellRadar listing data into clean, entity-resolved,
and analytics-ready datasets using Apache Spark.

---

## Objectives

The Big Data Processing pipeline performs:

1. Schema normalization
2. Data cleaning and text preprocessing
3. Duplicate handling
4. Entity resolution
5. Repost detection
6. Feature engineering
7. Generation of curated analytics datasets

---

# Pipeline

```text
Raw Data (Mercari TSV + Generated JSONL + manual Uploads JSONL)
   ↓
Schema Normalization (19-field union)
   ↓
Data Cleaning
   ↓
Duplicate Removal
   ↓
Entity Resolution
   ↓
Repost Detection
   ↓
Feature Engineering
   ↓
Curated Analytics Datasets
   ↓
Acceptance checks + dashboard/analytics consumers
```

---

# 1. Data Cleaning & Normalization

### File

```text
spark_jobs/clean_normalize.py
```

### Input

The pipeline combines data from:

* Mercari dataset (`hdfs://namenode:9000/data/raw/mercari/train.tsv`, real)
* Generated ResellRadar listings (`data/raw/generated/run=2026_09_28T150245Z/*.jsonl`, synthetic)
* Manual uploads (`data/raw/uploads/run=<ts>/*.jsonl`, optional operator-supplied third source)

The uploads zone is optional: the job probes its HDFS path through the Hadoop FileSystem
glob API before reading, so an absent/empty zone never fails the stage. When present, the
uploaded rows are normalized into the same 19-field union and stamped
`source_platform = "uploads"`. The upload contract lives in `docs/SCHEMA_uploads.md`.

### Common Schema

The datasets are normalized into a common 19-field schema:

```text
listing_id
title
description
price
price_raw
currency
category
sub_category
category_full
item_condition_id
brand_name
shipping
posted_date
delisted_date
location_city
location_region
seller_type
seller_id
source_platform
```

Additional processing columns (`title_clean`, `description_clean`) are created later in the stage.

### Cleaning Operations

* Standardized column names and data types
* Converted prices to numeric values
* Normalized category fields
* Converted invalid description markers such as `[rm]` to null
* Created cleaned title and description fields (lowercase, punctuation stripped)
* Removed records missing essential fields (`listing_id`, `title`, `price`)
* Removed duplicate listing IDs (`dropDuplicates(["listing_id"])`)

### Result

```text
Raw union records:     1,982,535   (Mercari 1,482,535 + generated 500,000)
Clean records:         1,972,679
Records removed:           9,856   (generated rows with a null price by design)
Duplicate listing_ids:         0
```

Output:

```text
data/processed/clean_listings.parquet    (21 columns)
```

---

# 2. Entity Resolution

### File

```text
spark_jobs/entity_resolution.py
```

Entity resolution identifies listings that potentially represent the same underlying product/model.

### Processing

The accepted job (replaces the originally uploaded MinHashLSH version after the
2026-09-29 acceptance run — see `docs/FINAL_REPORT.md` and `docs/HANDOFF_Person2.md`):

* Normalizes product titles (`title_clean`), collapses whitespace, tokenizes
* Removes each row's own city and the listing **chatter vocabulary**
* Builds the entity key as the **sorted token set** of the remaining title — storage,
  colour, and model are kept because they ARE the product identity
  (`docs/SCHEMA_generated.md`: canonical product = brand x model x storage x colour)
* Uses the vocabulary learned from the corpus by `scripts/learn_chatter_vocab.py`
  (`data/processed/chatter_vocab.json`, per `source_platform`; generated 53 tokens,
  Mercari 492 tokens). The job fails loudly if the artifact is missing.
* Assigns deterministic entity IDs (`row_number()` over the sorted model keys)
* Creates repost-related keys and detects potential reposts on
  seller + title key + posting dates (1–14 day window), linking each repost to an
  earlier original listing
* No MinHashLSH self-join — the display-only `approxSimilarityJoin` (17.3 billion
  candidate pairs, never consumed) was deleted

### Output

```text
data/processed/entity_resolved.parquet   (24 columns)
```

### Results

```text
Input records:       1,972,679
Output records:      1,972,679
Unique entities:     1,040,681
Detected reposts:       49,788
```

No records were lost during entity resolution.

Measured against the generator's ground truth (`scripts/evaluate_er.py`,
`logs/er_report_stage2_v3.md`):

| Task | Score |
|---|---|
| Same-product clustering F1 | **0.9802** (precision 1.0000, recall 0.9611) |
| Repost detection F1 | **0.9826** |
| Repost → original link F1 | **0.9621** |

The recall ceiling (0.9611) is coverage, not clustering: the 9,856 null-price rows that
Stage 1 drops by contract have no prediction.

---

# 3. Feature Engineering

### File

```text
spark_jobs/feature_engineering.py
```

The feature engineering stage generates aggregated analytics datasets for the dashboard,
all keyed on the resolved `entity_id`.

---

## 3.1 Depreciation Curve

Groups listings by:

```text
entity_id
listing_age_months   (months since the entity's first observed listing)
```

Generated metrics per entity and age:

* Listing count
* Average price
* Median price
* Baseline price (average at age 0)
* Price change percent vs baseline

Output:

```text
data/curated/depreciation_curve_curated.parquet
```

Current output:

```text
1,062,905 entity x age records
```

Caveat: Mercari rows carry no dates, so the time columns are meaningful only on the
generated/dated slice — filter `source_platform` when visualizing.

---

## 3.2 Resale Velocity

Calculates the time between:

```text
posted_date → delisted_date
```

Generated metrics per entity:

* Delisted listings count
* Average days to resale
* Median days to resale

Output:

```text
data/curated/resale_velocity_curated.parquet
```

Current output:

```text
1,105 entity records (roughly one per product with sales history)
```

---

## 3.3 Regional Price Variance

Groups listings by:

```text
entity_id
location_region
```

Generated metrics:

* Listing count
* Average price
* Price standard deviation
* Minimum / maximum regional price
* Regional price range

Output:

```text
data/curated/regional_price_variance_curated.parquet
```

Current output:

```text
1,056,069 entity x region records
```

Caveat: Mercari rows carry no location, so the regional columns are meaningful only on
the generated/dated slice.

---

# Final Dataset Outputs

| Dataset                                   | Purpose                            |
| ----------------------------------------- | ---------------------------------- |
| `clean_listings.parquet`                  | Clean normalized listing data      |
| `entity_resolved.parquet`                 | Entity-resolved listing-level data |
| `depreciation_curve_curated.parquet`      | Depreciation analytics             |
| `resale_velocity_curated.parquet`         | Resale speed analytics             |
| `regional_price_variance_curated.parquet` | Regional pricing analytics         |
| `chatter_vocab.json`                      | ER vocabulary artifact (Stage 2 input) |

---

# Dataset Sharing

The generated Parquet datasets are **not stored in GitHub** because of their size.
They are **rebuildable artifacts**: regenerate them from the raw zones with the commands
below, and find them locally in `data/processed/` + `data/curated/` and on HDFS in
`/data/processed/` + `/data/curated/` (pushed by `scripts/push_zones_to_hdfs.py`; the
distributed pipeline writes them there directly).

The processing scripts are available in GitHub.

---

# GitHub Branch

Development happens on `main`:

```text
main

spark_jobs/
├── clean_normalize.py
├── entity_resolution.py
└── feature_engineering.py
scripts/
├── learn_chatter_vocab.py   # builds chatter_vocab.json (run before Stage 2)
├── accept_pipeline.py       # structural acceptance checks
├── evaluate_er.py           # ER grading vs ground truth
└── run_stage_in_docker.sh   # Spark execution harness (single-unit / CLUSTER=1)
```

---

# Running the Pipeline

From the project root (HDFS stack up: `docker compose -f docker-compose.yml up -d`):

### Stage 0 — learn the ER vocabulary (required before Stage 2)

```bash
python scripts/learn_chatter_vocab.py
```

### Stage 1 — cleaning and normalization

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
```

### Stage 2 — entity resolution

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
```

### Stage 3 — feature engineering

```bash
bash scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
```

The harness runs the unmodified job files inside the Spark 4.2.0 container that can reach
the HDFS cluster (`hdfs://namenode:9000`). Add `CLUSTER=1` to the command to submit the
same stage to the Spark Standalone cluster (executors on the worker units) — see
`docs/CLUSTER_BATCH_MODE.md`.

### Gates

```bash
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python scripts/evaluate_er.py --pred data/processed/entity_resolved.parquet
```

The dashboard's **Start pipeline** button runs all of the above automatically (generation →
validation → HDFS push → Spark stages → acceptance), distributed when the cluster profile is up.

---

# Final Processing Summary

```text
Raw Records
1,982,535   (Mercari 1,482,535 + generated 500,000; uploads add when present)
       ↓
Clean Records
1,972,679   (21 columns)
       ↓
Entity-Resolved Records
1,972,679   (24 columns)
       ↓
Unique Entities
1,040,681
       ↓
Detected Reposts
49,788   (F1 0.9826)
       ↓
Curated Analytics
├── Depreciation Curve          1,062,905 rows
├── Resale Velocity             1,105 rows
└── Regional Price Variance     1,056,069 rows
       ↓
Acceptance: 24/24 PASS (logs/acceptance_checks.md)
```

---

# Handoff to Person 3

Person 3 should:

1. Pull `main`.
2. Regenerate the Parquet datasets (they are not stored in git) or obtain them from the
   HDFS zones / local `data/` as agreed by the team.
3. Keep the existing `data/processed/` and `data/curated/` folder structure.
4. Use `entity_resolved.parquet` for listing-level analytics.
5. Use the three curated datasets for dashboard visualizations.
6. Avoid modifying the original processed datasets.
7. Create separate derived datasets/views if additional dashboard-specific transformations are required.

The analytics page (`dashboard/app/analytics/page.tsx`) already consumes these tables
through `GET /api/analytics` (the FastAPI server aggregates them into KPIs, chart series,
and insights). The Big Data Processing stage is complete and integrated.

*Note*: `dashboard/graphs.py` is a leftover Streamlit prototype (imports helpers that do
not exist); the live dashboard is the Next.js app under `dashboard/`. Do not wire the
Streamlit file back in.
