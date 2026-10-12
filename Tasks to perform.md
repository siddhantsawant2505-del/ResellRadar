# ResellRadar — Task Plan v3 (2026-09-28)

## Person 1 — Data Engineer (Acquisition/Generation + Storage)
Owns: data download, synthetic generator, HDFS push, raw-zone validation, schema contracts.
Deliverable: ~2M raw listings (Mercari real + generated) landed in an immutable raw zone and
HDFS, with documentation, checksums, and full-data validation.

## Sources
1. REAL — Mercari Price Suggestion Challenge `train.tsv` (~1.48M rows, USD, no dates/location).
2. GENERATED — ~500k phone/furniture listings supplying the fields Mercari lacks
   (posted_date, delisted_date, location_city, location_region).

## Decisions (locked)
- **USD everywhere.** Messy price strings live only in `price_raw`; numeric `price` stays float.
- US city aliases (NYC/New York, SF/San Francisco, LA/Los Angeles, Philly/Philadelphia) instead
  of Indian aliases, since the price scale comes from a USD market. Same normalization teaching value.
- Raw zone partitioned per run: `data/raw/generated/run=<UTC timestamp>/gen_YYYY_MM_DD_batchNNN.jsonl`.
- Schema docs drafted BEFORE the generator; the generator implements the contract.
- Structural validation (gate) is separate from statistical profiling (report).

## Tasks
0. Housekeeping — .gitignore (data zones), requirements (kaggle, py7zr, psutil), legacy parking. ✅
1. `scripts/download_data.py` — use existing archives in `data/` first, then Kaggle CLI
   (rules-acceptance + credentials handling), extract to `data/raw/mercari/`, SHA-256 →
   `logs/checksums.txt`, row-count assert (~1.48M).
2. `scripts/extract_seed_sample.py` — chunked phone/furniture sample of Mercari into
   `data/seed/mercari_phone_furniture_sample.csv` (generator calibration input).
3. `docs/SCHEMA_mercari.md`, `docs/SCHEMA_generated.md`, `docs/SCHEMA_MISMATCHES.md` → then
   `generator/generate_data.py` (vectorized, seeded, 10k-chunk JSONL, all messiness targets).
4. `scripts/push_to_hdfs.py` — WebHDFS/CLI/emulated modes, replication=1, 64MB blocks,
   `logs/ingestion.log`, total blocks + size summary.
5. `scripts/validate_raw.py` — dual-profile (TSV + JSONL) chunked validator →
   `logs/validation_report.md`.
6. Smoke test at 10k rows, then full scale; record runtime + memory.
7. (LAST, out of scope for now) Stitch-generated ingestion control panel wired to real scripts.

## Status update (2026-10-05)

- Tasks 0-6: complete (download → seed sample → schemas → generator → HDFS push → dual-source
  validation → full-scale run), plus the quality gates (`qa_generated.py` 27/27, structural
  gate 0 violations, checksum seals).
- Task 7 (UI): complete and beyond — `server.py` orchestrates the full pipeline (:8000) and
  `dashboard/` is the Next.js ingestion console + analytics page, both in active use.
- Pipeline accepted end-to-end: 1,982,535 raw → 1,972,679 clean → 1,040,681 entities /
  49,788 reposts → curated 1,062,905 / 1,105 / 1,056,069; acceptance **24/24**
  (`logs/acceptance_checks.md`); distributed batch mode verified on the Spark Standalone
  cluster (`docs/CLUSTER_BATCH_MODE.md`).
- Added after v3: **manual upload ingestion** as a third raw source — console "Manual
  ingestion" panel, `scripts/ingest_uploads.py`, merge = skip-generation pipeline rerun
  (`docs/SCHEMA_uploads.md`).

## Legacy (pre-v3) components
- `scraper/` (Scrapy spider + old generator) and `hdfs_uploader.py` — superseded (the
  Mercari dataset replaced scraping); parked in git history / the local archive.
- `server.py` and `dashboard/` — **no longer legacy**: they were rebuilt as the FastAPI
  orchestration API and the Next.js console/analytics UI (task 7 above is complete).

## Cross-team note (addressed 2026-09-29)
The raw contract changes — per-source files (Mercari TSV + generated JSONL + optional
uploads JSONL), new `price_raw` field, nullable dates/location in Mercari rows — were
migrated: `spark_jobs/clean_normalize.py` now reads the TSV/JSONL paths with the 19-field
nullable union schema. Full defect/fix trail: `docs/HANDOFF_Person2.md`;
results: `docs/FINAL_REPORT.md`.
