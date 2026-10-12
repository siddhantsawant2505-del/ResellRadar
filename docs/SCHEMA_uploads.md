# SCHEMA — Manual Uploads (third source)

**Status**: OPERATOR-SUPPLIED data, ingested on demand (dashboard panel or CLI)
**Produced by**: `scripts/ingest_uploads.py` (normalize + validate), called from
`server.py` (`POST /api/ingest/upload`) or directly:

```bash
python scripts/ingest_uploads.py --file my_batch.csv [--source-name ebay-dump]
```

**Land zone**: `data/raw/uploads/run=<UTC ts>/<label>.jsonl` plus a sibling
`ingest_summary.json` — one `run=` partition per upload, never overwritten
(the same immutability rule as the other raw zones).
**Format**: JSON Lines after normalization; the *input* may be CSV, TSV, JSON or JSONL
(≤ 200 MB per file, streamed in 1 MiB chunks by the API).

This zone exists so the team can inject its own marketplace data (a CSV export, a
scraped batch, a hand-made test set) into the same pipeline the generated and Mercari
sources flow through. It joins the union in `spark_jobs/clean_normalize.py` as a third
source with `source_platform = "uploads"` — see `docs/SCHEMA_MISMATCHES.md`.

## Accepted input

| Input | Notes |
|---|---|
| CSV / TSV | First row is the header. Header names are normalized (lowercase, spaces → `_`, punctuation dropped) and mapped through the alias table below. |
| JSON | Root must be an array of objects (a `{"items": [...]}` wrapper is unwrapped); non-object items are rejected. |
| JSONL | One JSON object per line; a malformed line is counted as one rejected row. |

### Header aliases → canonical field

| Input header (normalized) | Canonical field |
|---|---|
| `train_id`, `id` | `listing_id` |
| `name` | `title` |
| `item_description` | `description` |
| `category_name` | `category_full` |
| `item_condition`, `condition` | `item_condition_id` |
| `brand` | `brand_name` |
| `merchant_rating` | `seller_type` |

Any other header that already matches a canonical field name passes through unchanged;
columns the union does not know are ignored.

## Normalization + validation rules

| Rule | Behavior |
|---|---|
| `title` | Required. Empty/missing → row rejected (`missing title`). Truncated to 300 chars. |
| `price` | Required. Parsed from messy strings (`$1,200`, `1200`, `$1.2k`, `$1,200/-`); blank/`negotiable`/non-positive → row rejected (`missing or invalid price`). |
| `item_condition_id` | Coerced to int 1–5; out-of-range or non-integer → NULL (row kept). |
| `shipping` | Coerced to 0/1; anything else → NULL. |
| `posted_date` / `delisted_date` | Parsed from `YYYY-MM-DD[ HH:MM:SS]`, `MM/DD/YYYY`, `DD-MM-YYYY`; unparseable → NULL (never rejects the row). |
| `currency` | Defaults to `USD` (upper-cased, ≤ 8 chars). |
| `listing_id` | Used as-is when present. When absent, a deterministic id is generated: `up_<source label>_<hash(title, price, seller_id)>`. |
| Duplicates | A row whose `listing_id` already exists in the **Mercari or generated zones** (the ingest scan covers those two sources: `_existing_listing_ids()`), or earlier in the same batch, is **skipped and counted** (`duplicates_of_existing`) — an upload can never duplicate the canonical corpus. IDs are *not* re-scanned across upload runs, so re-ingesting the same file creates a new `run=` partition; Stage 4 unions every upload partition and then `dropDuplicates(["listing_id"])`, so the processed tables never contain duplicates. |
| Everything else | Missing optional fields (`description`, `brand_name`, `category*`, `seller_*`, dates, location) become NULL. |

Rows are normalized into the full 19-field union schema (the same list in
`docs/SCHEMA_generated.md` / `docs/SCHEMA_mercari.md`). Only two fields differ by
nature: `source_platform` is always `"uploads"`, and `price` is never null (uploaded
rows without a usable price are rejected rather than stored).

## Storage + merge contract

```text
data/raw/uploads/run=<UTC ts>/
├── <label>.jsonl          # normalized rows, 19-field union schema
└── ingest_summary.json    # accepted / rejected / duplicates / samples / source file
```

- **Storage**: the standard raw-zone push (`python scripts/push_to_hdfs.py`) walks
  `data/raw/uploads/` like every other raw partition, so the zone lands at
  `hdfs://namenode:9000/data/raw/uploads/run=<ts>/...` and is logged in
  `logs/ingestion.log` + `data/hdfs_sync_manifest.json`.
- **Merge**: the "Ingest & merge" panel action (or `rerun=true` on the API) reruns the
  whole pipeline with `skip_generation` — no new synthetic batches are generated; the
  uploaded batch is pushed to HDFS and unioned as the third source in Stage 4. The
  processed/curated tables are a deterministic full recompute of the raw zone, so
  "merge with the already processed data" means: upload → union → recompute → new
  processed tables.
- **Acceptance**: `scripts/accept_pipeline.py` DERIVES its expected counts from the raw
  zone at runtime (it reads the canonical generated run out of `clean_normalize.py` and
  counts every upload file), so an uploaded batch *moves* the expectations instead of
  failing the gate. An empty uploads zone is a valid state (`uploads=0 expected 0`).
- **Optional zone**: Stage 4 probes the uploads path with the Hadoop FileSystem glob API
  before reading, so an absent/empty zone never fails the Spark job.

## Evidence

- `logs/acceptance_checks.md` — the stage-1 check line
  `all sources survived the union with the expected row counts` reports
  `mercari=…, generated=…, uploads=…` for the current state (the committed canonical run
  is `uploads=0 (expected 0)`).
- `logs/ingestion.log` — upload partitions pushed to HDFS (one line per file with row
  counts, bytes, blocks).
- The panel lists the last 24 h of upload runs (accepted / rejected / duplicates per run)
  via `GET /api/ingest/uploads`.
