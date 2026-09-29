# ResellRadar — Final Project Report

**Project**: ResellRadar — resale-market data lake + Spark pipeline
**Report date**: 2026-09-29  |  **Generator run**: `run=2026_09_28T150245Z` (seed 42)
**Repo**: `main` @ `68d15f8` (synced with origin)
**Status**: pipeline complete, accepted end-to-end, all evidence committed

---

## 1. Executive summary

ResellRadar ingests ~2M resale marketplace listings (1.48M real Mercari rows + 500k
generated rows that supply the fields real data lacks), lands them in an immutable raw
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
verified through Spark (`logs/hdfs_zone_readback.txt`).

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

All three Spark jobs run `.master("local[*]")` — parallel across all 16 host cores
(Stage outputs carry 16–17 part-files, i.e. genuinely parallel partition writers). The
Hadoop cluster provides storage, not execution: there is no YARN/worker layer, so this is
multi-core parallelism on one machine, not a multi-node execution cluster — the documented
scale trade-off for ~2M rows.

---

## 3. Data contracts

| Zone / artifact | Contract |
|---|---|
| Mercari source | `docs/SCHEMA_mercari.md` — no dates/location/seller by nature; brand nulls ~43% published baseline |
| Generated source | `docs/SCHEMA_generated.md` — canonical product = brand x model x storage x colour; titles are 6–15 surface variants; documented messiness (condition words, extras, repost suffixes); 1,108 canonical products; 50k designed reposts (10%) |
| Cross-source | `docs/SCHEMA_MISMATCHES.md` — dates/location/seller_id are **generated-only**; all downstream consumers must segment per source |
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

---

## 5. Storage — what is on the cluster

| HDFS path | Contents | Size |
|---|---|---|
| `/data/raw` | immutable source zone (1 TSV + 50 JSONL, checksummed) | 620.6 MB |
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
3. **Single-node execution** — real multi-core parallelism (`local[*]`, 16–17 partition
   writers) but no YARN/worker layer; HDFS is storage-only (1 DataNode, replication 1).
4. **F3** — the entity-id `row_number()` window flows ~1.1M keys through one partition;
   deterministic, seconds of cost, documented in the job header; revisit at ~100x scale.
5. **Semantic guard** — the learned vocabulary's one hand-written piece: 11 product-line
   suffix tokens that statistics alone would wrongly delete.
6. **Dashboard** — the ingestion console UI exists but its `/api` routes are not yet
   implemented, and the Streamlit `graphs.py` still imports missing modules written against
   a stale schema; wiring it to the real curated tables is the one open deliverable.

---

## 7. Evidence index (every claim has a file)

**Master reports (start here)**
- `logs/acceptance_2026_09_29.md` — full acceptance narrative: defects, fixes, parity tables, v3 section, adoption record
- `logs/validation_report.md` — dual-source profiling; Mercari baselines PASS (brand nulls 42.68% ~ published ~43%; category 0.43% ~ ~0.4%)
- `logs/acceptance_checks.md` / `logs/acceptance_checks_v3.md` — structural checker output (stage1 9/9, stage2 9/9, stage3 6/6)

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
- `logs/hdfs_zone_ingestion.txt` / `logs/hdfs_zone_readback.txt` — processed/curated push + Spark read-back
- `logs/ingestion.log` — raw push manifest log; `logs/checksums.txt` — SHA-256 per raw file

**Documentation**
- `docs/SCHEMA_mercari.md`, `docs/SCHEMA_generated.md`, `docs/SCHEMA_MISMATCHES.md` — contracts
- `docs/HDFS_SETUP.md` — cluster setup, healthcheck fix, three-zone push/read-back workflow
- `docs/SPARK_SETUP.md` — Spark execution environment
- `docs/HANDOFF_Person2.md` — defect/fix detail for the original Stage 2 owner (4c)

**Tooling (all committed)**
- `scripts/accept_pipeline.py` — structural checks per stage
- `scripts/evaluate_er.py` — pairwise P/R/F1 + repost/link grading (self-tested; `entity_id` alias; NaN-cluster bug fixed)
- `scripts/learn_chatter_vocab.py` — corpus vocabulary learner
- `scripts/push_to_hdfs.py` / `scripts/push_zones_to_hdfs.py` — raw / zone pushes (fail loudly, no emulation)
- `scripts/verify_hdfs_zones.py` — Spark read-back proof
- `scripts/run_stage_in_docker.sh` + `docker/spark/Dockerfile` — execution harness
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
