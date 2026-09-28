# Pipeline acceptance run - Person 2's Spark jobs vs the v2 raw contract

**Date**: 2026-09-29  |  **Reviewer**: Person 1

**Verdict**: `clean_normalize.py` **accepted**; `feature_engineering.py` **accepted**;
`entity_resolution.py` **as uploaded cannot complete** (dead 17.4-billion-pair join) and its
entity definition was wrong - **fixed in `spark_jobs/entity_resolution_v2.py`**, which now
scores **0.9802** on the same-product task instead of **0.2341**.

Artifacts reviewed (as uploaded, mtimes 2026-09-29 00:02):

| File | Size | Result |
|---|---|---|
| `spark_jobs/clean_normalize.py` | 7,269 B | runs, 9/9 acceptance checks |
| `spark_jobs/entity_resolution.py` | 10,419 B | never finishes; entity semantics wrong |
| `spark_jobs/feature_engineering.py` | 7,889 B | runs, 6/6 acceptance checks |

## 0. How the jobs were executed (and why not on the host)

The jobs hardcode `hdfs://namenode:9000`. On this Windows host that endpoint is not usable:
the host cannot resolve the compose-internal hostnames `namenode`/`datanode`
(`getaddrinfo failed`), the DataNode data-transfer port 9866 is not published
(`127.0.0.1:9866` refused; container IP `172.18.0.3:9866` times out), and host-run Spark
needs `winutils.exe` before it can write parquet at all (`HADOOP_HOME and
hadoop.home.dir are unset`).

So the job files were run **unmodified** inside a Spark 4.2.0 container joined to the
compose network - the environment they were written for (in-network DNS, in-network block
transfer, Linux, matching pyspark version):

```bash
docker compose up -d && docker compose up -d --no-deps datanode   # see F5
scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
scripts/run_stage_in_docker.sh spark_jobs/entity_resolution_v2.py
scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
```

`scripts/run_stage_in_docker.sh` and `docker/spark/Dockerfile` (stock `apache/spark:4.2.0`
+ `numpy`, which `pyspark.ml` needs but the stock image lacks) are new, harness-only
additions.

## 1. Stage 1 `clean_normalize.py` - ACCEPTED

Input read from HDFS: `train.tsv` 337.8 MB + 50 JSONL 312.8 MB.

- Total records 1,982,535 = 1,482,535 real Mercari + 500,000 generated (exact).
- Clean records 1,972,679, written to `data/processed/clean_listings.parquet` (392 MB).

The 9,856 dropped rows are **exactly** the generated rows carrying a null `price`
(counted directly from the 50 JSONL files: `price_null = 9,856`; Mercari has zero null
title/price/listing_id). `dropna(subset=["listing_id","title","price"])` therefore honours
the documented ~2% messiness contract with no unintended loss.

`scripts/accept_pipeline.py stage1` -> **9/9 PASS**, notably:

- both sources survive with the exact expected counts (mercari 1,482,535 / generated
  490,144);
- `source_platform`, `listing_id`, `title`, `price` never null; no duplicate `listing_id`;
- Mercari rows carry NULL `posted_date`/`delisted_date`/`seller_id`/`location_city`;
- generated rows keep their dates and `seller_id`;
- Mercari brand null rate **42.68%**, matching the published ~43% baseline in
  `logs/validation_report.md`;
- `category_full` nulls (6,327 = 0.43%) are Mercari-only, matching the documented ~0.4%
  missing `category_name`; generated rows never null;
- `title_clean` is lowercased/alphanumeric for all 1,972,679 rows.

## 2. Stage 2 as uploaded - TWO DEFECTS

### F1 - the job never finishes; the blocking join is dead code

Started twice; never got past `========== SIMILAR MODEL PAIRS ==========` (16m32s at 15-19
of 16 cores, heap pinned near the cap, no output - the job sets log level ERROR, so there
is no progress to watch).

Measured root cause (`logs/diag_lsh_candidates.txt`): the model set fed to `MinHashLSH` is
`distinct(title_clean, model_key)` = **1,170,504 rows**, and with `numHashTables=5` (Spark
implements this as 5 bands of a single hash each) the `approxSimilarityJoin` candidate set is

```
CANDIDATE_PAIRS_TOTAL 17,360,494,325      (17.4 billion)
per band: 4.17B / 2.34B / 3.90B / 4.19B / 2.75B
biggest single bucket: 55,513 members -> C(55513,2) = 1.54 billion pairs from one bucket
```

No memory or partition setting makes a 17-billion-row self-join complete here.

Worse, the result is **never used**: `pairs` is filtered and `show(30)`n, but `entity_id` is
assigned from `row_number().over(Window.orderBy("model_key"))` over `distinct(model_key)`.
Proof from the run output:

```
Unique model keys: 1,109,646
Unique Entities:   1,109,646      <- identical: the LSH changed nothing
```

### F2 - the entity definition destroyed the product identity

`docs/SCHEMA_generated.md` defines a canonical product as `brand x model x storage x colour`
(phones) / `type x material x size` (furniture), with titles as **surface variants** of it
("6-15 surface variants per canonical product"). The job's regexes strip the
*product-defining* parts - storage sizes and colours - and keep the listing chatter:

```
canonical P:Apple|iPhone 13|128GB|Titanium  ->  80 different model_keys across 955 listings
canonical P:Apple|iPhone XR|64GB|Lavender   ->  79 different model_keys across 934 listings
```

Measured on the graded subset: **39.1 model_keys per true product on average** (median 42,
max 80), and only **3 of 1,107** products were covered by a single key. That is the whole
recall story, and stripping `black`/`white`/`128gb` also merges *different* products, which
is the precision story.

### 2b. Interim acceptance copy

`scripts/entity_resolution_lsh_bounded.py` is a byte-identical copy of the uploaded job with
one bounded block (`demo_df = model_key_df.sample(fraction=0.02, seed=42)` for the
display-only pairs). It completes and writes `entity_resolved.parquet`
(1,109,646 entities, 49,788 reposts), which separated "does the logic run" from "does the
dead join finish". Its grade is the baseline row in the table below.

## 3. The fix - `spark_jobs/entity_resolution_v2.py`

Three changes, same input path, same output path, same schema (so Stage 3 and the dashboard
need no change):

1. **Entity key = product tokens, not a destructively stripped string.** Tokenize
   `title_clean`, remove the listing's own `location_city`, remove the documented chatter
   vocabulary (condition words `for/parts/poor/fair/good/like/new`; the `TITLE_EXTRAS`
   `with/box/clean/imei/no/scratches/screen/protector/on/factory/unlocked/esim/ready`; the
   template fillers `used/fs/selling/pickup/must/go/relist`), then key on the sorted
   token **set**. Everything else - storage, colour, model - is kept because it *is* the
   product. Set semantics also collapse the generator's "last-token-first" variant.
2. **The dead MinHash LSH self-join is deleted** (and with it the `pyspark.ml`/`numpy`
   dependency) - the demo was the entire reason the job never finished.
3. **No title indirection.** v1 mapped title -> entity and joined on `title_clean`, which
   silently duplicates rows when one title maps to two entities. The key is a per-row
   function, so listings join the entity table directly on `model_key`.
   (Repost detection is copied verbatim, so tasks B and C are unaffected.)

One implementation trap, worth recording because the prototype missed it: `clean_normalize`
strips punctuation but leaves the spaces around it, so the `" - Unlocked"` /
`" - {city} pickup"` / `" - {condition}"` templates leave a **double space**. Python's
`str.split()` collapses whitespace; Spark's `split(col, " ")` emits an **empty token** that
no chatter list removes - and every product split into exactly 2 entities (mean 1.98,
max 2). v2 collapses whitespace before splitting and drops `""` defensively.

### Result - entity level

| | v1 (as delivered) | v2 (fixed) |
|---|---|---|
| entities | 1,109,646 | 1,065,540 |
| entities per true product (graded subset) | mean **39.1**, max 80 | mean **1.00**, max 1 |
| products covered by exactly 1 entity | 3 / 1,107 | **1,107 / 1,107** |
| entities mixing more than one product | - | **0** |
| runtime | never finished (16m32s+) | ~4 min |

### Result - accuracy vs ground truth (`logs/er_report_stage2_v2.md`)

`python scripts/evaluate_er.py --pred data/processed/entity_resolved.parquet`

| Task | v1 (bounded copy) | **v2 (fixed)** |
|---|---|---|
| A - same product (pairwise P/R/F1) | 0.5017 / 0.1527 / **0.2341** | 1.0000 / 0.9611 / **0.9802** |
| B - repost detection | 0.9847 / 0.9805 / 0.9826 | 0.9847 / 0.9805 / **0.9826** |
| C - repost -> original link | 0.9716 / 0.9527 / 0.9621 | 0.9716 / 0.9527 / **0.9621** |

Predicted same-cluster pairs 171,178,690 == true same-canonical pairs among covered rows:
the clustering is now **exact**, with no false merges at all. The remaining recall loss is
*not* an ER error - it is coverage: 9,856 truth listings (the null-price rows Stage 1 drops)
have no prediction, so recall is capped at 98.03% coverage = 0.9611. Fixing that would mean
changing Stage 1's documented `dropna` contract, not Stage 2.

Tasks B and C are byte-for-byte the uploaded algorithm's scores, confirming the fix did not
disturb repost detection.

`scripts/accept_pipeline.py stage2` -> **9/9 PASS** (24 columns, no temp columns left, row
count unchanged, no duplicate ids, every row entity-tagged, reposts self-consistent, never
self-linked, no dangling original ids).

### Honest caveat on the 0.9802

The chatter vocabulary is taken from the **documented** messiness spec
(`docs/SCHEMA_generated.md` + the generator's `COND_WORDS` / `TITLE_EXTRAS` /
`REPOST_SUFFIXES`), which is the team's own data contract rather than the answer key - no
truth column is read at runtime. But it is a *fixed* vocabulary, so this score is an upper
bound for synthetic data with a finite chatter generator. On the real Mercari half of the
corpus the same idea needs a learned/extended vocabulary (or a bounded similarity pass), and
the number should be expected to be lower there.

## 4. Stage 3 `feature_engineering.py` - ACCEPTED (re-run on the v2 entities)

Exit 0, three tables written to `data/curated/`:

| Table | Rows | Columns |
|---|---|---|
| `depreciation_curve_curated.parquet` | 1,087,764 | entity_id, listing_age_months, listing_count, average_price, median_price, baseline_price, price_change_percent |
| `resale_velocity_curated.parquet` | 1,105 | entity_id, delisted_listings, avg_resale_days, median_resale_days |
| `regional_price_variance_curated.parquet` | 1,080,928 | entity_id, location_region, listing_count, average_price, price_stddev, min_regional_price, max_regional_price, regional_price_range |

`scripts/accept_pipeline.py stage3` -> **6/6 PASS**.

Entity granularity visibly improved the output: `resale_velocity` went from 31,118 fragmented
rows to **1,105** rows, i.e. roughly one row per real product with sales history.

Caveats for the dashboard (expected from the source contract, but they shape every chart):

- `depreciation_curve`: **97.9%** null `listing_age_months`/`baseline_price`/
  `price_change_percent` - time features exist only for generated rows, and merged entities
  are mostly Mercari listings.
- `regional_price_variance`: **98.5%** null `location_region`, **88.1%** null `price_stddev`
  - Mercari has no location, so the table is mostly a NULL-region group.
- Both must be filtered to `source_platform = 'generated'` (or segmented per source) before
  being presented as "market" numbers. This is the documented `SCHEMA_MISMATCHES` split, not
  a bug.

## 5. Fixes made to Person 1's own tooling

- `scripts/evaluate_er.py`: accepts **`entity_id`** as a canonical-cluster alias (the column
  the Stage 2 job actually emits).
- `scripts/evaluate_er.py`: fixed a real scoring bug - the task-A intersection used
  `groupby(..., dropna=False)`, so rows **without** a prediction formed a phantom NaN cluster
  and their within-cluster pairs were counted as correct (this is why the earlier v1 grade
  printed precision 1.0006 > 1). The v1 baseline row above was produced before this fix; the
  effect is < 0.001 (the phantom group is 9,856 rows), so the comparison stands, but v2's
  numbers are the corrected ones.

## 6. Remaining items (non-blocking)

1. **F3**: `row_number().over(Window.orderBy("model_key"))` has no `partitionBy`, so all
   ~1.1M keys pass through a single partition. It works, but it is a serialization point.
2. **Chatter vocabulary portability** - see the caveat in 3.
3. **Unowned decision**: who merges `entity_resolution_v2.py` into
   `spark_jobs/entity_resolution.py` (Person 2's file was left untouched so the fix can be
   diffed).
4. **F5 (environment)**: after a Docker restart the compose stack deadlocks - the NameNode
   waits in safe mode for a DataNode, while the DataNode waits on `depends_on:
   service_healthy`. Bring it up with `docker compose up -d --no-deps datanode`.

## 7. Reproduce

```bash
docker compose up -d && docker compose up -d --no-deps datanode   # cluster
scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
scripts/run_stage_in_docker.sh spark_jobs/entity_resolution_v2.py
scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
python scripts/evaluate_er.py --pred data/processed/entity_resolved.parquet
```

Evidence files (`.txt` because `.gitignore` excludes `*.log`): `logs/acceptance_stage1.txt`,
`logs/acceptance_stage2_as_uploaded_stall.txt` (the 16m32s as-uploaded stall),
`logs/acceptance_stage2_bounded.txt`, `logs/acceptance_stage2_v2.txt`,
`logs/acceptance_stage3.txt`, `logs/diag_lsh_candidates.txt` (the 17.4B measurement), plus
`logs/acceptance_checks.md`, `logs/er_report_stage2_bounded.md`, `logs/er_report_stage2_v2.md`.
Generated parquet outputs stay out of git (`data/processed/*`, `data/curated/*`).
