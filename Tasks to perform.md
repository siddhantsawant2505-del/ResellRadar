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

## Legacy (pre-v3) components
- `scraper/` (Scrapy spider + old generator), `hdfs_uploader.py`, `server.py`, `dashboard/`,
  old `README.md` — superseded; kept until the UI task replaces them. See README_PERSON1_NEXT_STEPS.md.

## Cross-team note (send to Person 2 NOW)
The raw contract changes: per-source JSONL (Mercari TSV or converted JSONL + generated JSONL),
new `price_raw` field, nullable dates/location in Mercari rows. `spark_jobs/clean_normalize.py`
has a 14-field non-nullable StructType and a hardcoded JSONL path that must be migrated.
