# ResellRadar — Person 1: v2/v3 Migration Notes (2026-09-28)

The repo previously implemented a Scrapy-based 50k ingestion pipeline (v1). The plan moved to a
two-source design: the real **Mercari Price Suggestion Challenge** dataset (Kaggle, ~1.48M rows)
plus a **~500k vectorized synthetic generator**. This file records what is legacy and why.

## Legacy (parked, not deleted)
| File / dir | Why parked |
|---|---|
| `scraper/spider.py`, `scraper/config.py` | Live scraping replaced by the Kaggle dataset (reproducible, rules-safe). |
| `scraper/generator.py` | Row-by-row, 30-day window, no messiness — superseded by `generator/generate_data.py`. |
| `scraper/validator.py` | Hard-fails on blank prices; v2 separates structural gate from statistical profiling (`scripts/validate_raw.py`). |
| `scraper/jsonl_converter.py` | Exists only to bridge v1's JSON-array batches; v2 writes JSONL chunks directly. |
| `hdfs_uploader.py` | Replaced by `scripts/push_to_hdfs.py` (fixes per-file WebHDFS path bug; adds replication=1, 64MB blocks, record counts, blocks summary). |
| `server.py`, `dashboard/` | v1 control panel; will be rebuilt (task 7) wired to the real v2 scripts. Do not run: imports the old generator. |

## New layout (Person 1, v2/v3)
```
scripts/download_data.py      # archives-first Kaggle download + checksums
scripts/extract_seed_sample.py# Mercari phone/furniture sample for calibration
scripts/push_to_hdfs.py       # HDFS push + logs/ingestion.log + blocks summary
scripts/validate_raw.py       # chunked dual-source validation -> logs/validation_report.md
generator/generate_data.py    # vectorized, seeded, 10k-chunk JSONL generator
docs/SCHEMA_mercari.md        # real source contract
docs/SCHEMA_generated.md      # generated source contract
docs/SCHEMA_MISMATCHES.md     # union/mismatch contract for Person 2
data/raw/mercari/             # extracted real data (immutable)
data/raw/generated/           # run=<ts>/gen_YYYY_MM_DD_batchNNN.jsonl (immutable)
data/seed/                    # calibration sample
logs/                         # checksums.txt, ingestion.log, validation_report.md
```

## Data status
- `data/train.tsv.7z` (77.9 MB) present in `data/` — Mercari archives downloaded manually.
  `scripts/download_data.py` uses existing archives first; Kaggle CLI is the fallback.
- Raw-zone immutability: never modify files after writing; generated batches go to per-run
  partitions so re-runs never overwrite.
- Generator v2 (2026-09-28): 1,108-canonical catalog (phones model x storage x 8 colors,
  furniture type x material x size), `seller_id` (50k Zipf-skewed pool), ~10% designed
  reposts (same seller, near-identical title, posted 1–14d later), and a ground-truth
  parquet at `data/ground_truth/run=<ts>/truth.parquet` (outside the raw zone) carrying
  `listing_id, true_canonical_id, is_repost, original_listing_id`. Old v1 run archived at
  `data/raw/archive/generator_v1/`. Canonical brands below the seed-sample threshold
  (Google, Motorola, Xiaomi) get a 2.5% fallback share so every canonical is realizable.
- Real HDFS: `docker-compose.yml` (apache/hadoop:3, namenode + datanode, replication 1,
  64 MB blocks) — setup in `docs/HDFS_SETUP.md`; no emulation fallback in the push script.

## Scale benchmarks (2026-09-28, generator v2, Python 3.12 / pandas 2.2.2 / numpy 2.4.6, Windows)

| Step | 10k-row smoke | Full scale (500k gen / 1.48M real) |
|---|---|---|
| `download_data.py` (extract + verify) | n/a | extract ~30s; row-count verify ~10s |
| `generate_data.py` | 0.5s, peak RSS ~110 MB | **9.8s, peak RSS 227 MB, ~51k rows/s, 312.8 MB / 50 files + truth.parquet 3.65 MB** |
| `qa_generated.py` (27 checks incl. truth) | 0.9s | 8.3s (full 500k) — 27/27 PASS |
| `validate_raw.py` (both sources) | 0.2s (gen only) | **17.7s, peak RSS 204 MB, 1,982,535 rows profiled** |
| `push_to_hdfs.py` (REAL cluster) | - | 51 files, 650.7 MB, 56 blocks @ 64 MB, replication 1; fsck HEALTHY |

Memory stays flat with scale by design: the generator holds one 10k chunk at a time and the
validator accumulates only aggregates (plus one float array per source for exact percentiles).
Mercari baseline checks both PASS (brand_name nulls 42.68% vs published ~43%; category_name
nulls 0.43% vs ~0.4%), confirming a byte-correct parse. Duplicate semantics (v2): designed
rate = same seller_id + normalized title within 14 days (8.15% measured; signature merging
compresses the 10% designed reposts); the naive (title, price) rate (1.15%) is reported
separately as an upper bound dominated by chance collisions. HDFS is now a real
single-node pseudo-distributed cluster in Docker — see `docs/HDFS_SETUP.md`.
