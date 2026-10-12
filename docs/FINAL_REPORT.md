# ResellRadar — Final Project Report

**Project**: ResellRadar — resale-market data lake + Spark pipeline
**Report date**: 2026-10-12 (first issued 2026-09-29; pipeline evidence as of the 2026-10-05 canonical run)  |  **Generator run**: `run=2026_09_28T150245Z` (seed 42)
**Repo**: `main` @ `a13abe7` (upload-ingestion feature committed; `git log --oneline`)
**Status**: pipeline complete, accepted end-to-end (24/24), manual upload ingestion integrated,
dashboard + analytics live, distributed cluster batch verified

---

## 1. Executive summary

ResellRadar ingests ~2M resale marketplace listings (1.48M real Mercari rows + 500k
generated rows that supply the fields real data lacks, plus any operator-supplied upload
batches), lands them in an immutable raw
zone on a real single-node HDFS cluster, and processes them through a three-stage Spark
pipeline into curated analytics tables:

| Stage | Job | Verdict |
|---|---|---|
| 1 — clean & normalize | `spark_jobs/clean_normalize.py` | **accepted**, 9/9 structural checks |
| 2 — entity resolution + repost detection | `spark_jobs/entity_resolution.py` | **accepted** after rewrite; same-product pairwise F1 **0.9802** (P 1.0000 / R 0.9611); repost F1 0.9826; link F1 0.9621 |
| 3 — feature engineering | `spark_jobs/feature_engineering.py` | **accepted**, 6/6 structural checks |

The headline engineering story is Stage 2: the originally delivered job could never finish
(a display-only MinHashLSH join costing **17,360,494,325** candidate pairs) and its entity
key destroyed the product identity (F1 **0.2341**). The accepted version keys entities on
the sorted token set of the title minus a chatter vocabulary that is **learned from the
corpus itself** — no generator constants, no ground truth at runtime — which recovers
F1 **0.9802** and generalizes to real marketplace titles (492 learned Mercari chatter
tokens; 24,859 additional real titles merged).

The full lake (raw → processed → curated) lives on HDFS and every zone was read back and
verified through Spark (`logs/hdfs_zone_readback.txt`; the restore run's read-back is
`logs/hdfs_zone_readback_restore.txt`).

Since 2026-10-05 the lake also accepts **manual uploads**: the console's *Manual ingestion*
panel (or `scripts/ingest_uploads.py`) normalizes an operator's CSV/TSV/JSON/JSONL into a
third raw zone (`data/raw/uploads/run=<ts>/`, `source_platform="uploads"`), and a
"merge" rerun of the pipeline unions that batch into the same processed/curated tables.
The upload contract is `docs/SCHEMA_uploads.md`; the mechanics are in section 4 (upload feature).

---

## 2. Architecture — the three-zone lake

```
 SOURCES                      RAW ZONE (immutable)                 PROCESSED ZONE                CURATED ZONE
 ------------------           ------------------------------      ------------------------      --------------------------
 Mercari train.tsv      --+
 (1,482,535 rows, TSV)    |    /data/raw/mercari/                STAGE 1                       STAGE 3
                          +-->   train.tsv  (337.8 MB,         clean_normalize.py            feature_engineering.py
 generator/generate_data    |          6 x 64 MB blocks)        -------------------------     -------------------------
 (500,000 rows JSONL,     |                                      /data/processed/              /data/curated/
 seeded, run=<ts>)        +-->  /data/raw/generated/            clean_listings.parquet        depreciation_curve_curated.parquet
                          |      run=2026_09_28T150245Z/         (1,972,679 rows, 392 MB)      (1,062,905 rows)
                          |      gen_*.jsonl  (50 files,                                       resale_velocity_curated.parquet
                          |      312.8 MB)                       STAGE 2                       (1,105 rows)
                          |                                      entity_resolution.py          regional_price_variance_curated.parquet
                          |                                      -------------------------     (1,056,069 rows)
                          |                                      /data/processed/
                          |                                      entity_resolved.parquet       consumers:
                          |                                      (1,972,679 rows,              dashboards / reports
                          |                                       1,040,681 entities)          (filter source_platform!)
                          |
                          +-->  chatter_vocab.json (11 KB)  <---- learned artifact
                                                                 (scripts/learn_chatter_vocab.py,
                                                                  reads Stage 1 output only)
```

Data flows strictly left to right. The raw zone is **immutable** (fixes = new `run=`
partition, never edits; SHA-256 pinned in `logs/checksums.txt`). The processed and
curated zones are rebuildable artifacts, re-derived from raw at any time.

A third, optional source lands in the same raw zone: **manual uploads**
(`data/raw/uploads/run=<ts>/`, `source_platform="uploads"`) union into Stage 1 on the next
run — mechanics in section 4, contract in `docs/SCHEMA_uploads.md`.

### Execution topology

```
 Windows 11 host (Git Bash, Python 3.12)
 |
 |-- docker compose: resellradar-namenode + resellradar-datanode   (apache/hadoop:3, 3.3.6)
 |     HDFS: replication 1, 64 MB blocks, ports 9870 / 9000 / 9864
 |     volumes: hdfs_namenode, hdfs_datanode  (data survives restarts; healthcheck fixed)
 |
 |-- Spark execution harness: scripts/run_stage_in_docker.sh
       runs the UNMODIFIED job files in apache/spark:4.2.0 (+numpy) joined to the
       compose network, repo bind-mounted at /work
       -> why: jobs address hdfs://namenode:9000, which only resolves in-network;
          host-side Spark additionally needs winutils on Windows
```

Execution has two modes with identical job files and identical results (distributed run
verified to the exact row count, 2026-10-05):

- **Single-unit (default)** — `.master("local[*]")`, parallel across all 16 host cores in
  one JVM (stage outputs carry 16–17 part-files, i.e. genuinely parallel writers).
- **Distributed batch** — Spark Standalone cluster via the compose `cluster` profile:
  master + 3 worker units, 3 executor JVMs x 3 cores, reading/writing the HDFS zones.
  Run: `CLUSTER=1 scripts/run_stage_in_docker.sh <job>` (docs/CLUSTER_BATCH_MODE.md).
  The full pipeline ran distributed on 2026-10-05 with every canonical count reproduced
  (evidence: `logs/cluster_batch_run.txt`).

---

## 3. Data contracts

| Zone / artifact | Contract |
|---|---|
| Mercari source | `docs/SCHEMA_mercari.md` — no dates/location/seller by nature; brand nulls ~43% published baseline |
| Generated source | `docs/SCHEMA_generated.md` — canonical product = brand x model x storage x colour; titles are 6–15 surface variants; documented messiness (condition words, extras, repost suffixes); 1,108 canonical products; 50k designed reposts (10%) |
| Cross-source | `docs/SCHEMA_MISMATCHES.md` — the three-source union contract; dates/location/seller_id are **generated-only** by design (uploads carry them only when the file does); all downstream consumers must segment per source |
| Manual uploads | `docs/SCHEMA_uploads.md` — CSV/TSV/JSON/JSONL ≤ 200 MB, `title` + `price` required, dedup against the whole raw zone on ingest, `source_platform="uploads"` |
| Ground truth | `data/ground_truth/run=2026_09_28T150245Z/truth.parquet` — 500k rows, 1:1 via `listing_id`; used **only** by the grader for reporting, never as a pipeline input or feature |

---

## 4. Stage results

### Stage 1 — clean & normalize (accepted 9/9)

- In: 1,982,535 raw rows (TSV + 50 JSONL). Out: **1,972,679** clean rows (392 MB parquet).
- The 9,856 dropped rows are exactly the generated rows with null `price` — the
  documented ~2% messiness contract, no unintended loss.
- Mercari 1,482,535 / generated 490,144, exact; brand nulls 42.68% (~43% documented);
  `category_full` nulls 6,327 = 0.43% (~0.4% documented), Mercari-only.
- Mercari rows keep NULL dates/location/seller; generated rows keep theirs.
- The production job also unions the optional `data/raw/uploads/` zone (its HDFS path is
  probed with the Hadoop FS glob API before reading, so an empty/absent zone is safe). With
  no uploads the counts above are unchanged; the stage-1 check reports
  `uploads=0 (expected 0)`.

### Stage 2 — entity resolution + repost detection (accepted 9/9)

Defects found in the delivered job (both measured, `logs/acceptance_2026_09_29.md`):

- **F1 (blocking)**: `approxSimilarityJoin` produced 17,360,494,325 candidate pairs yet was
  display-only dead code (`Unique model keys == Unique Entities` proved it fed nothing) —
  the job never finished (16m32s stall captured in `logs/acceptance_stage2_as_uploaded_stall.txt`).
- **F2 (semantic)**: the `model_key` regexes stripped storage/colour — the product identity —
  and kept listing chatter: mean **39.1** keys per true product, Task A F1 **0.2341**.

The accepted job (`spark_jobs/entity_resolution.py`):

1. Entity key = sorted token **set** of `title_clean` minus the row's own city minus a
   learned chatter vocabulary (keeps storage/colour/model — they ARE the product).
2. The dead LSH self-join is gone; listings join the entity table directly on the key
   (no title indirection, no duplicate-row hazard).
3. Whitespace is collapsed before tokenizing (the clean templates leave double spaces;
   Spark `split(c, " ")` then emits an empty token that split every product in two).
4. **The chatter vocabulary is learned, not hardcoded** (`scripts/learn_chatter_vocab.py`
   -> `data/processed/chatter_vocab.json`): a token/phrase is chatter iff *deleting it from
   a title yields the exact sorted token set of another real title from the same source* in
   >= 85% of containing titles (>= 50-title support, 1–3-token phrases). Product-line
   suffixes (`pro/max/plus/mini/ultra/se/air/edge/note/fe/lite`) are guarded — surface-
   optional in text but catalog identity (unguarded prototype measured P 0.9621).
   - generated: **53 tokens** learned — rediscovers all 27 documented ones (27/27) + 26 city names
   - mercari: **492 tokens** learned — real marketplace chatter (`bnwt`, `wristlet`,
     `distressed`, ...) no fixed list could anticipate
   - the job fails loudly if the artifact is missing; ground truth is never read

| Metric | delivered job | accepted job |
|---|---|---|
| Runtime | never finished | ~4 min |
| Entities | 1,109,646 (fragmented) | **1,040,681** |
| Entities per true product (graded subset) | mean 39.1, max 80 | mean 1.00 |
| Products covered by exactly 1 entity | 3 / 1,107 | **1,107 / 1,107** |
| Entities mixing >1 product | — | **0** |
| Task A same-product F1 | 0.2341 | **0.9802** (P 1.0000 / R 0.9611) |
| Task B repost F1 | 0.9826 | 0.9826 (block unchanged) |
| Task C repost->original link F1 | 0.9621 | 0.9621 (block unchanged) |

The recall ceiling 0.9611 = 98.03% coverage: the 9,856 null-price rows Stage 1 drops have
no prediction (a Stage 1 contract decision, not an ER error).

### Stage 3 — feature engineering (accepted 6/6, re-run on final entities)

| Table | Rows | Caveat (documented generated-only fields) |
|---|---|---|
| `depreciation_curve_curated.parquet` | 1,062,905 | ~97.9% null time columns — Mercari has no dates; filter `source_platform='generated'` |
| `resale_velocity_curated.parquet` | 1,105 | healthy — roughly one row per product with sales history |
| `regional_price_variance_curated.parquet` | 1,056,069 | ~98.5% null region, ~88.1% null stddev — Mercari has no location |

### Feature — manual upload ingestion & merge (added 2026-10-05)

The raw zone accepts operator-supplied batches through `scripts/ingest_uploads.py`
(`POST /api/ingest/upload`, behind the dashboard's **Manual ingestion** panel):

- **Input**: CSV / TSV / JSON / JSONL, ≤ 200 MB, normalized to the 19-field union;
  `title` + `price` are required, condition/shipping are coerced or NULL, unparseable
  dates become NULL, and header aliases (`name`, `item_description`, `category_name`,
  `brand`, `condition`, `merchant_rating`, …) are mapped to the canonical fields.
- **No duplicates in the processed corpus**: a row whose `listing_id` already exists in the
  Mercari/generated zones (or earlier in the same file) is skipped at ingest, and Stage 4
  unions every upload partition and then `dropDuplicates(["listing_id"])`.
- **Landing**: `data/raw/uploads/run=<ts>/<label>.jsonl` + `ingest_summary.json`; the
  standard raw-zone push carries it to `hdfs://namenode:9000/data/raw/uploads/...`.
- **Merge**: "Ingest & merge" reruns the pipeline with `skip_generation=true` (no new
  synthetic batches). Stage 3 pushes the zone, Stage 4 unions it, HDFS outputs sync back,
  and Stage 7 acceptance **derives its expected counts from the raw zone at runtime** — an
  uploaded batch moves the expectations instead of failing the gate.
- **Verified in both directions** during development: a 3-row test batch merged
  (Stage 4 printed `Manual Upload Records: 3`; acceptance reported
  `uploads=3 (expected 3)`), and after the test batch was purged a rebuild reproduced the
  canonical corpus. The committed `logs/acceptance_checks.md` is that canonical state
  (`uploads=0 (expected 0)`, 24/24 PASS).
- Two real defects were found and fixed by this feature: Spark 4 validates glob paths
  eagerly, so an empty optional zone threw `PATH_NOT_FOUND` before the guarded read (now
  probed with the Hadoop FS API first), and the acceptance derivation had a leftover
  variable in the uploads loop.
- **Limits**: 200 MB per file; a merge is a full recompute (~8 min), not an incremental
  append; the Spark cluster must be up for a merge. Contract: `docs/SCHEMA_uploads.md`.

---

## 5. Storage — what is on the cluster

| HDFS path | Contents | Size |
|---|---|---|
| `/data/raw` | immutable source zone: 1 Mercari TSV + 50 canonical generated JSONL + 8 console test batches (not part of the union); manual uploads land here too | 682.9 MB (canonical files 650.7 MB) |
| `/data/processed` | clean_listings (21 cols), entity_resolved (24 cols), chatter_vocab.json | ~787 MB |
| `/data/curated` | 3 curated analytics tables | ~17.5 MB |

Zone push: `scripts/push_zones_to_hdfs.py` (same connection ladder + fail-loudly policy as
the raw-zone push; 804.5 MB / 17 blocks, log `logs/hdfs_zone_ingestion.txt`). Read-back
proof: `scripts/verify_hdfs_zones.py` reads every zone through Spark inside the cluster
network — all 5 tables OK with exact row/col parity (`logs/hdfs_zone_readback.txt`), which
also proves blocks are healthy. Cluster survives full restarts with data intact
(healthcheck deadlock fixed; fsck HEALTHY, 0 missing / 0 corrupt).

---

## 6. Known limitations (honest, all documented)

1. **Coverage cap** — Task A recall 0.9611 is capped by Stage 1's documented null-price
   drop contract. Lifting it is a team contract decision, not a bug.
2. **Generated-only analytics fields** — depreciation/region charts are meaningful only on
   the generated subset; dashboards must filter per source.
3. **Cluster is one physical host** — the distributed batch mode runs real Spark
   Standalone executor units (3 workers x 3 cores) over HDFS, but every unit is a Docker
   container on one machine with 1 DataNode (replication 1); no physical multi-node
   locality, and single-unit `local[*]` remains the default mode.
4. **F3** — the entity-id `row_number()` window flows ~1.1M keys through one partition;
   deterministic, seconds of cost, documented in the job header; revisit at ~100x scale.
5. **Semantic guard** — the learned vocabulary's one hand-written piece: 11 product-line
   suffix tokens that statistics alone would wrongly delete.
6. **Dashboard** — implemented. The Next.js console now has live telemetry, the pipeline
   trigger/stop, the streaming log terminal, the Spark cluster panel, the manual-upload
   panel, HDFS sync and the raw data grid; the analytics page renders KPIs, charts and
   auto-generated insights from the processed parquet tables, and every `/api/*` route is a
   thin same-origin proxy to `server.py`. `dashboard/graphs.py` remains dead Streamlit
   prototype code (it imports `data_loader` and `charts`, which do not exist) and should be
   pruned rather than wired in.
7. **Zone hygiene** — the generated zone (local + HDFS) also holds 8 console-trigger test
   batches (`run=2026_10_05*`, 51,500 rows) whose `listing_id`s collide with the canonical
   run. The union ignores them: Stage 1 reads only the pinned canonical partition, and
   `scripts/accept_pipeline.py` mirrors that pin. `logs/validation_report.md` profiles the
   whole zone, so its generated row count reads 551,500 rather than the canonical 500,000.
8. **Manual uploads are operator-supplied** — date/location/brand/seller coverage depends
   entirely on the uploaded file, so upload rows must stay segmented as their own source in
   every aggregation. A merged run is a deterministic full recompute of the raw zone
   (minutes), not an incremental append, and it requires the Spark cluster up.

---

## 7. Evidence index (every claim has a file)

**Master reports (start here)**
- `logs/acceptance_2026_09_29.md` — full acceptance narrative: defects, fixes, parity tables, v3 section, adoption record
- `logs/validation_report.md` — raw-zone profiling (dual source). With Mercari included, baselines PASS (brand nulls 42.68% ~ published ~43%; category 0.43% ~ ~0.4%). The automated pipeline's Stage 2 runs it with `--skip-mercari` (Mercari is static and checksum-pinned), so pipeline-regenerated copies profile the generated zone only; the file also covers the 8 console test batches that are outside the union (limitation 7)
- `logs/acceptance_checks.md` — current structural checker output, 24/24 PASS (stage1 9/9 incl. `uploads=0 (expected 0)`, stage2 9/9, stage3 6/6)
- `logs/acceptance_checks_v3.md` — the v3-adoption snapshot of the same checker

**Grading**
- `logs/er_report_stage2_v3.md` — final grade (A 0.9802 / B 0.9826 / C 0.9621)
- `logs/er_report_stage2_v2.md` — fixed-list parity grade (identical Task A)
- `logs/er_report_stage2_bounded.md` — as-delivered baseline (0.2341)

**Stage run logs**
- `logs/acceptance_stage1.txt`, `logs/acceptance_stage2_v3.txt` (canonical Stage 2),
  `logs/acceptance_stage3_v3.txt`, plus the historical
  `acceptance_stage2_as_uploaded_stall.txt` (16m32s stall) and
  `acceptance_stage2_bounded.txt`

**Diagnostics & learning**
- `logs/diag_lsh_candidates.txt` — the 17,360,494,325-pair measurement
- `logs/learn_chatter_vocab.txt` — learned vocabularies (53 generated / 492 mercari), parameters, coverage diagnostics

**Storage & cluster**
- `logs/hdfs_proof.txt` — raw-zone ls / du / fsck (HEALTHY, 0 missing blocks)
- `logs/hdfs_zone_ingestion.txt` / `logs/hdfs_zone_readback.txt` / `logs/hdfs_zone_readback_restore.txt` — processed/curated push + Spark read-back (the `_restore` copy is the post-cleanup rebuild)
- `logs/ingestion.log` — raw push manifest log; `logs/checksums.txt` — SHA-256 per raw file

**Distributed batch execution (2026-10-05)**
- `logs/cluster_batch_run.txt` — one-page evidence: 3 workers x 3 cores registered, 4 applications on the standalone master, HDFS zone sizes, replication note
- `logs/cluster_stage1.txt` / `logs/cluster_stage2.txt` / `logs/cluster_stage3.txt` / `logs/cluster_verify.txt` — per-stage driver logs (3 executors each, canonical counts, exit 0)
- `logs/local_regression_stage3.txt` — default single-unit mode still passes after the change
- `docs/CLUSTER_BATCH_MODE.md` — how to run, size and verify the cluster

**Documentation**
- `docs/SCHEMA_mercari.md`, `docs/SCHEMA_generated.md`, `docs/SCHEMA_uploads.md`, `docs/SCHEMA_MISMATCHES.md` — source + union contracts
- `docs/HDFS_SETUP.md` — cluster setup, healthcheck fix, three-zone push/read-back workflow
- `docs/SPARK_SETUP.md` — Spark execution environment
- `docs/HANDOFF_Person2.md` — defect/fix detail for the original Stage 2 owner (4c)

**Tooling (all committed)**
- `scripts/accept_pipeline.py` — structural checks per stage
- `scripts/evaluate_er.py` — pairwise P/R/F1 + repost/link grading (self-tested; `entity_id` alias; NaN-cluster bug fixed)
- `scripts/learn_chatter_vocab.py` — corpus vocabulary learner
- `scripts/push_to_hdfs.py` / `scripts/push_zones_to_hdfs.py` — raw / zone pushes (fail loudly, no emulation)
- `scripts/verify_hdfs_zones.py` — Spark read-back proof
- `scripts/run_stage_in_docker.sh` + `docker/spark/Dockerfile` — execution harness (single-unit and `CLUSTER=1` distributed modes)
- `server.py` — pipeline orchestration API (:8000) behind the Next.js dashboard; "Start pipeline" auto-detects the standalone cluster and submits Spark stages as driver containers (`resellradar-spark-console-*`), syncs HDFS outputs back before acceptance, exposes `GET /api/cluster` (workers/running/finished apps), and serves manual ingestion (`POST /api/ingest/upload`, `GET /api/ingest/uploads`) plus analytics aggregates (`GET /api/analytics`); see `docs/CLUSTER_BATCH_MODE.md`
- `scripts/ingest_uploads.py` + `dashboard/components/UploadPanel.tsx` — manual upload ingestion (normalize/validate into the raw zone; merge rerun with `skip_generation`)
- `scripts/entity_resolution_lsh_bounded.py` — preserved as-delivered copy (bounded demo) for diffing
- `scripts/qa_generated.py` — 27/27 generator QA incl. truth-file integrity

---

## 8. Reproduce from scratch

```bash
# cluster
docker compose -f docker-compose.yml up -d
docker ps                          # namenode (healthy), datanode (up)

# raw zone
python scripts/download_data.py                    # sources + checksums
python scripts/extract_seed_sample.py              # generator calibration input
python generator/generate_data.py                  # seeded synthetic run
python scripts/validate_raw.py                     # dual-source profiling gate
python scripts/qa_generated.py                     # 27/27 generator QA
python scripts/push_to_hdfs.py --fresh

# optional: manual ingestion (third raw source) — normalizes a batch into data/raw/uploads/
# and it merges on the next pipeline run (skip_generation mode, e.g. the dashboard's "Ingest & merge")
python scripts/ingest_uploads.py --file my_batch.csv --source-name demo
python scripts/push_to_hdfs.py

# pipeline (stages run unmodified in the Spark container)
python scripts/learn_chatter_vocab.py              # learn chatter vocabulary FIRST
bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
bash scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
bash scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py

# gates
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python scripts/evaluate_er.py --pred data/processed/entity_resolved.parquet

# complete the lake on HDFS + prove it
python scripts/push_zones_to_hdfs.py
bash scripts/run_stage_in_docker.sh scripts/verify_hdfs_zones.py
```

---

## 9. Commit history (this project arc)

| Commit | Content |
|---|---|
| `511ab22` | HDFS cluster config XMLs (gitignore rule un-anchored, files were silently untracked) |
| `2a80375` | Acceptance run: v2 entity resolution (F1 0.2341 -> 0.9802), harness, grader fixes, evidence |
| `0187823` | Namenode healthcheck fix — restart-with-data no longer deadlocks |
| `772ed8f` | Learned chatter vocabulary (v3): removability method, per-source vocab, re-grade |
| `9b74b53` | v3 adopted as canonical `entity_resolution.py`; processed/curated zones landed on HDFS + read-back proof |
| `68d15f8` | Legacy components pruned (scraper/, server.py, hdfs_uploader.py) |
| `877ef2b` | Final project report (this document) |
| `7f93840` | Dashboard/analytics integration: `/api/*` proxy routes + analytics page, Spark cluster panel and `/api/cluster` telemetry, compose `cluster` profile, `docs/CLUSTER_BATCH_MODE.md`, README rewrite |
| `2d9420b` | Upload feature: `scripts/ingest_uploads.py`, `POST /api/ingest/upload`, `UploadPanel`, Stage-1 uploads union + Hadoop-FS probe, runtime-derived acceptance expectations, `skip_generation` trigger |
| `a13abe7` | Canonical restore run evidence: acceptance 24/24 with `uploads=0`, regenerated validation report |
