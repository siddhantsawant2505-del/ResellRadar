# HANDOFF — Person 1 → Person 2 (raw zone is ready)

**Date**: 2026-09-28  |  **Generator**: v2  |  **Run**: `run=2026_09_28T150245Z` (seed 42)
**Status**: ~2M rows landed (1,482,535 real Mercari + 500,000 generated), validated, on a
**real** single-node HDFS cluster. This page is the fast path; the binding contracts are
`docs/SCHEMA_generated.md` + `docs/SCHEMA_MISMATCHES.md`.

**Acceptance run 2026-09-29** (your uploaded jobs, executed against the live cluster):
`clean_normalize.py` and `feature_engineering.py` are **accepted** (9/9 and 6/6 checks).
`entity_resolution.py` **cannot run as uploaded**: its `approxSimilarityJoin` self-join is
dead code (it never feeds `entity_id`) yet costs **17,360,494,325 candidate pairs**, and its
entity key strips the product identity (storage/colour) instead of the listing chatter.
Fixed copy: **`spark_jobs/entity_resolution_v2.py`** - same input path, output path and
schema - same-product F1 **0.2341 -> 0.9802**, repost scores unchanged. Full evidence:
`logs/acceptance_2026_09_29.md`; details in section 4c. Same day, **v3**
(`spark_jobs/entity_resolution_v3.py` + `scripts/learn_chatter_vocab.py`) removed v2's one
remaining limitation - the fixed 27-token chatter list - by learning a per-source vocabulary
from the corpus itself: same **0.9802**, 24,859 more real-Mercari titles merged (end of
section 4c).

## 1. Where the data lives

| Zone | Local | HDFS (live cluster) |
|---|---|---|
| Mercari (REAL) | `data/raw/mercari/train.tsv` (TSV, header) | `/data/raw/mercari/train.tsv` (6 blocks) |
| Generated (SYNTH) | `data/raw/generated/run=2026_09_28T150245Z/` (50 JSONL, 10k rows each) | `/data/raw/generated/run=2026_09_28T150245Z/gen_*.jsonl` (50 blocks) |

- Cluster: `docker-compose.yml` (apache/hadoop:3, namenode+datanode, replication 1, 64 MB
  blocks). Start: `docker compose -f docker-compose.yml up -d`, then read
  `hdfs://namenode:9000/data/raw/...`. Full guide: `docs/HDFS_SETUP.md`.
  In Spark: `spark.read.text/csv/json("hdfs://namenode:9000/data/raw/...")`.
- Health proof: `logs/hdfs_proof.txt` (ls / du / fsck - all HEALTHY, 0 missing blocks).
  Ingestion log: `logs/ingestion.log`. Integrity: `logs/checksums.txt` (SHA-256 per file).

## 2. What changed vs your current `clean_normalize.py` (MUST migrate)

**Audit verdict (2026-09-28): none of your three jobs can run against the current
pipeline as-is.** Blockers, in order: (1) Stage 1 reads `data/processed/raw_listings.jsonl`,
which no longer exists (that was the v1 scraper handoff; the raw zone is now
`data/raw/mercari/train.tsv` + `data/raw/generated/run=<ts>/*.jsonl`, or HDFS). (2) The
14-field non-nullable StructType both drops your new fields (seller_id, price_raw,
item_condition_id, brand_name, shipping, category_full) and kills every real Mercari row
at the `dropna` (Mercari has no dates/location by nature). (3) Stage 2 selects a
`scraped_at` column that no longer exists in the union. Also note: the `hadoop/` Windows
helper binaries your setup doc references are not in the repo, and the data volume is now
~2M rows, not 50k. Migration list below stands; the HDFS cluster is live and is the
recommended input source.

Your job has a 14-field non-nullable StructType and a hardcoded path - both are stale.

1. **Union schema is 19 fields.** Added since your version: `price_raw` (Mercari: NULL),
   `currency` (constant `USD`), `category_full` (never null in generated),
   **`seller_id`** (generated-only `S-XXXXX`; Mercari rows NULL - never impute),
   `location_city`/`location_region` (generated-only), `posted_date`/`delisted_date`
   (generated-only; null = still active).
2. **Nullability**: use nullable types everywhere except `source_platform`. Mercari has
   NULL dates/location/seller; generated has ~5% null `description`, ~5% null
   `sub_category`, ~2% null `price` (by design - do not "fix").
3. **Path fix**: read Mercari from the TSV (`sep="\t"`, derive `source_platform="mercari"`)
   and generated from `run=<ts>/*.jsonl`; keep `source_platform` from the JSONL as-is.
4. **IDs**: `train_id` → cast to string; generated `listing_id` is `GEN-<batch>-<seq>`.
   Never pooled as one numeric key.
5. **`price_raw`** is Person 2's parsing exercise (generated rows only):
   `"$1,200"`, `"1200"`, `"$1.2k"`, `"$1,200/-"`, `"negotiable"`, `""`. Numeric `price`
   already exists and is null exactly when `price_raw` is blank/negotiable (~2%).

## 3. Ground truth (new artifact - read the rules)

`data/ground_truth/run=2026_09_28T150245Z/truth.parquet` - 500,000 rows, 1:1 with the
generated JSONL via `listing_id`:

| Column | Meaning |
|---|---|
| `true_canonical_id` | which of the 1,108 catalog products the listing depicts |
| `is_repost` | True for the 50,000 designed reposts (10.0%) |
| `original_listing_id` | the repost's original listing (never dangles); NULL for originals |

Rules: join on `listing_id` only; use it as **ER labels, never as model features**; it is
NOT part of the raw zone (gitignored, checksum-pinned); generated rows only.

## 4. Duplicate-detection contract

- Designed duplicates = **same `seller_id` + normalized title within 14 days**
  (near-identical titles carry suffixes like `" - must go"`, `" (relist)"`, `"!!"`).
- Validation measured: designed rate 8.15% (signature view of the true 10%), naive
  (title, price) rate 1.15% - that naive number is an upper bound dominated by chance
  collisions; see the note in `logs/validation_report.md`.
- Report duplicate rates **per source, never pooled** (Mercari dups are organic).

## 4b. Grading your ER output (new harness)

`scripts/evaluate_er.py` scores your entity-resolution output against the truth parquet
(pairwise clustering P/R/F1 computed exactly from cluster sizes; repost detection; and
repost-to-original link recovery when you provide links):

```bash
python scripts/evaluate_er.py --pred <your_output.parquet|csv>
```

Your file needs `listing_id` + a cluster column (`predicted_canonical_id`, or the
`pred_canonical_id` / `cluster_id` / **`entity_id`** aliases - `entity_id` is what
`spark_jobs/entity_resolution.py` actually emits, so it grades as-is, no rename step);
optionally `predicted_is_repost` and `predicted_original_listing_id`. Self-tested:
perfect predictions score 1.0000; scoring is deterministic across re-runs. `cluster_id`
may be your own opaque labels - any consistent grouping is scored correctly.

## 4c. Acceptance run 2026-09-29 - two defects, both fixed in `entity_resolution_v2.py`

**F1 (blocking: the job never finishes).** The `pairs` DataFrame from
`model.approxSimilarityJoin(...)` is filtered and `show(30)`n but never consumed - `entity_id`
comes from `row_number().over(Window.orderBy("model_key"))` over `distinct(model_key)`.
Proof from the run: `Unique model keys: 1,109,646` and `Unique Entities: 1,109,646`
(identical). On the delivered data the model set is 1,170,504 rows, so that display-only
self-join explodes to 17,360,494,325 candidate pairs (measured,
`logs/diag_lsh_candidates.txt`) and the job never leaves the `SIMILAR MODEL PAIRS` step
(16m32s, 16 cores, no output). Fix: delete the block, or bound the demo input.

**F2 (semantic: the entity key throws the product away).** `docs/SCHEMA_generated.md`
defines a canonical product as `brand x model x storage x colour` (phones) with titles as
surface variants of it - but the `model_key` regexes strip storage and colours, i.e. the
parts that ARE the product, and keep the listing chatter. Measured on the graded subset:
**39.1 model_keys per true product** (median 42, max 80); only 3 of 1,107 products were
covered by a single key. `scripts/evaluate_er.py` grades it at Task A F1 **0.2341**
(P 0.5017 / R 0.1527).

**Fix - `spark_jobs/entity_resolution_v2.py`** (same input path, output path and schema, so
Stage 3 and the dashboard are untouched):

1. entity key = the sorted token **set** of `title_clean`, minus the row's own
   `location_city`, minus the documented chatter vocabulary (condition words, `TITLE_EXTRAS`,
   and `used/fs/selling/pickup/must/go/relist`). Storage, colour and model are kept;
2. the dead LSH self-join is deleted (and with it the `pyspark.ml`/`numpy` dependency);
3. no title indirection - listings join the entity table on `model_key` directly, which also
   removes a latent duplicate-row bug;
4. whitespace is collapsed before tokenizing: `clean_normalize` leaves a double space behind
   the `" - Unlocked"` / `" - {city} pickup"` / `" - {condition}"` templates, and splitting
   on a literal space emitted an empty token that split every product into exactly 2 entities.

Repost detection is copied verbatim, so tasks B and C are unchanged.

| | v1 as uploaded | v2 fixed |
|---|---|---|
| runtime | never finished | ~4 min |
| entities per true product | mean 39.1, max 80 | **1.00, max 1** |
| products covered by exactly 1 entity | 3 / 1,107 | **1,107 / 1,107** |
| Task A same-product F1 | 0.2341 | **0.9802** (P 1.0000 / R 0.9611) |
| Task B repost F1 | 0.9826 | 0.9826 |
| Task C link F1 | 0.9621 | 0.9621 |

The leftover recall loss is coverage, not clustering: the 9,856 null-price rows that Stage 1
drops have no prediction, capping recall at 98.03%.

Your call now: adopt **v3** (or v2) into `entity_resolution.py` (your file was left untouched
so the fix can be diffed), or tell Person 1 which parts you want changed.

**Caveat on that 0.9802 (superseded the same day by v3).** The chatter list came from the
documented messiness spec, not from the answer key, but it was a *fixed* vocabulary - on the
real Mercari half a learned/extended one is needed. **Done in `spark_jobs/entity_resolution_v3.py`**:
the fixed 27-token list is replaced by a vocabulary learned from the corpus per
`source_platform` (`scripts/learn_chatter_vocab.py` -> `data/processed/chatter_vocab.json`).
Method: a token/phrase is chatter iff deleting it from a title yields the exact sorted token
set of another real title from the same source (>= 85% of containing titles, >= 50-title
support); product tokens never pass (every phone title names its colour/storage). A small
guard (`pro/max/plus/mini/ultra/se/air/edge/note/fe/lite`) keeps product-line suffixes that
statistics alone would delete and merge iPhone Pro with Pro Max - this is the only
hand-written piece. The job fails loudly if the artifact is missing; no truth is read at
learn or job time. Learned: generated 53 tokens (redelivers the 27 documented ones plus 26
city names), mercari 492 (real chatter: `bnwt`, `wristlet`, `distressed`, ...). Grade parity:
Task A 1.0000 / 0.9611 / **0.9802** identical to v2; entities 1,065,540 -> **1,040,681**
(-24,859 mercari merges nobody wrote down); structural checks 9/9. Stage 3 re-run on the v3
entities: depreciation 1,062,905 / velocity 1,105 / regional 1,056,069 rows.

**F3 (open, non-blocking).** `row_number().over(Window.orderBy("model_key"))` has no
`partitionBy`, so all ~1.1M keys pass through a single partition.

## 5. Evidence pack (for the report / viva)

- `logs/validation_report.md` - dual-source profiling, Mercari baselines PASS
  (brand nulls 42.68% ~ published ~43%; category nulls 0.43% ~ ~0.4%), structural gates 0.
- `logs/hdfs_proof.txt` - live-cluster ls/du/fsck outputs.
- `scripts/qa_generated.py` - 27/27 checks incl. truth-file integrity on the shipped run.

## 6. Reproducibility & immutability

- Raw zones are immutable: fixes/changes = new `run=` partition, never edits.
- Generator is seeded (42, per-batch seed+index); a re-run reproduces the same bytes for
  the same generator version. Old v1 run parked at `data/raw/archive/generator_v1/`
  (no `seller_id`, no truth file - do not use).

Questions → Person 1. Blockers on the union schema → update `docs/SCHEMA_MISMATCHES.md`
rather than silently diverging.
