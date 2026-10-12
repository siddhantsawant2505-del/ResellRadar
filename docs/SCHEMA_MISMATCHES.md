# SCHEMA — Mismatches & Union Contract

**Purpose**: the single page Person 2 reads before touching the raw data. It defines how the
three sources union, what is missing where, and which source is real vs generated vs
operator-supplied.

## Source status at a glance

| | Mercari `train.tsv` | Generated JSONL | Manual uploads JSONL |
|---|---|---|---|
| **Status** | **REAL** (Kaggle marketplace dump) | **GENERATED** (synthetic, seeded; generator v2) | **OPERATOR-SUPPLIED** (ingested on demand) |
| Rows | ~1,482,535 | canonical run 500,000 (may be more in the zone) | depends on the file |
| Landing zone | `data/raw/mercari/train.tsv` | `data/raw/generated/run=<ts>/gen_*_batchNNN.jsonl` | `data/raw/uploads/run=<ts>/<label>.jsonl` |
| Format | TSV (header row) | JSON Lines, 10k records/file | JSON Lines (CSV/TSV/JSON accepted at ingest) |
| Price | float USD (`0.0` rows exist) | float USD + messy `price_raw` string | float USD parsed from messy strings (required) |
| Dates | **None** | posted/delisted, ISO dates, 24-month span | per file (unparseable → NULL) |
| Location | **None** | city + region (US metros, aliases included) | per file |
| Seller identity | **None** | `seller_id` (50k pool, Zipf-skewed) | per file |
| Contract doc | `docs/SCHEMA_mercari.md` | `docs/SCHEMA_generated.md` | `docs/SCHEMA_uploads.md` |

## Union schema (Person 2's `clean_normalize.py` target)

Read all sources with the columns below. Assign `source_platform`:
`"mercari"` for the real TSV (derived constant — Mercari never had this field),
`"generated"` is already present in the JSONL, `"uploads"` for every manually ingested row.

| Column | Mercari | Generated | Uploads | Union rule |
|---|---|---|---|---|
| `listing_id` | `str(train_id)` | `listing_id` | `listing_id`, or generated `up_<label>_<hash>` when absent | cast to string; no overlap by construction (`GEN-` / `up_` prefixes vs digits); upload rows whose id already exists in the zone are skipped at ingest |
| `title` | `name` | `title` | `title` (or `name` alias) | direct |
| `description` | `item_description` (may be `[rm]` → treat as null) | nullable str | nullable str (or `item_description` alias) | normalize both to null-or-str |
| `price` | float USD | float USD (null ~2%) | float USD, parsed from messy strings | numeric in every source; **uploads never null** (rows without a usable price are rejected) |
| `price_raw` | null (add column) | str | original price string | Mercari rows get NULL — there is no raw string |
| `currency` | add constant `"USD"` | `"USD"` | defaults `"USD"` | direct |
| `category` | split `category_name` part 1 (`Electronics`, `Home`, …) | `category` | `category` | generated uses Mercari top-level labels so no remap |
| `sub_category` | split `category_name` part 3 | nullable str | nullable str | generated leaf matches Mercari leaf vocabulary |
| `category_full` | `category_name` (nullable) | `category_full` | `category_full` (or `category_name` alias) | exact pipe string where present; uploads may be NULL |
| `item_condition_id` | int 1–5 | int 1–5 | int 1–5, else NULL | direct (generated synthesizes the same scale) |
| `brand_name` | str, **~43% null** | str (never null) | nullable str (or `brand` alias) | **null-rate mismatch is real** — do not impute silently |
| `shipping` | 0/1 | 0/1 | 0/1, else NULL | direct |
| `posted_date` | **NULL for all rows** | ISO date | parsed ISO date or NULL | temporal analyses run on dated rows only (generated / uploads) |
| `delisted_date` | **NULL for all rows** | nullable ISO date | parsed ISO date or NULL | same; null = still active |
| `location_city` | **NULL for all rows** | str | nullable str | regional analyses run on dated/geo-tagged rows only |
| `location_region` | **NULL for all rows** | str | nullable str | same |
| `seller_type` | **NULL for all rows** | str | nullable str (or `merchant_rating` alias) | no ground truth in Mercari |
| `seller_id` | **NULL for all rows** (add column) | str `S-XXXXX` | nullable str | **generated-only field in the design**; uploads supply it only if the file has it. Duplicate detection on `seller_id` is not comparable across sources. |
| `source_platform` | constant `"mercari"` | constant `"generated"` | constant `"uploads"` | the only provenance flag — do not drop it |

## Hard mismatches (agreed handling)

1. **No dates/location in Mercari** — depreciation, resale velocity, and regional price
   variation are computed on the dated/geo sources (generated, and any uploads that carry
   those fields). Brand/condition price effects can use the
   full 1.48M real rows. If the team wants time-series over the union, the only acceptable path
   is a documented *derived enrichment* (columns suffixed `_synthetic`), never silent.
2. **`price` nullability differs**: Mercari never null, generated null ~2% (messiness spec).
   Person 2: use nullable DoubleType for the union.
3. **`brand_name` nullability differs**: ~43% null (real) vs 0% (generated) vs whatever the
   uploaded file carries. Any model or
   aggregation on brand must report coverage per source.
4. **`sub_category` nullability differs**: ~0.4% missing `category_name` in Mercari vs 3–7%
   injected nulls in generated. `category_full` is never null in generated — use it for joins.
5. **`description` semantics**: Mercari has `[rm]` placeholders; generated has 3–7% true nulls.
6. **Duplicate semantics differ**: generated reposts (~10%, 8–12% band) are *within-source*
   engineered duplicates sharing `seller_id` with a near-identical title posted 1–14 days
   later; Mercari's residual dups are organic; uploads are checked at ingest against the
   Mercari/generated corpus + their own batch by `listing_id` (first write wins), and Stage 4
   unions every upload partition then drops duplicate `listing_id`s. Report duplicate rates
   per source, never pooled.
   Generated-side duplicates are defined as **same `seller_id` + normalized title within
   14 days**; the naive (title, price) collision rate is also reported and is dominated by
   chance collisions at this scale (see `logs/validation_report.md`).
7. **IDs are not comparable** — `listing_id` is a union-safe string key only, not a join key
   to anything external.
8. **Ground truth lives outside the raw zone**: `data/ground_truth/run=<ts>/truth.parquet`
   carries `listing_id, true_canonical_id, is_repost, original_listing_id`. It is Person 2's
   entity-resolution answer key — join on `listing_id` only, never merge into the raw zone,
   never use as a model feature. Raw JSONL contains no leakage columns by design. (Uploaded
   rows have no ground truth — only the generated source does.)
9. **`seller_id` is generated-only by design**: there is no Mercari counterpart, so any
   seller-level aggregation must filter on `source_platform = 'generated'` (or run per-source
   as usual). Uploads supply it only when the file carries a `seller_id` column.
10. **Uploads are operator-supplied and self-serving**: the zone accepts whatever passes the
    ingest validation (`title` + `price` mandatory, condition/shipping/date coercion — see
    `docs/SCHEMA_uploads.md`), so its column coverage is unknown per file. Treat upload rows
    as their own source (`source_platform = 'uploads'`) in every aggregation, and remember
    that a merged run is a full deterministic recompute of the raw zone, not an incremental
    append. The zone is optional: with no uploads, the union and the acceptance gate behave
    exactly as before (`uploads=0`).

## Immutability & provenance

- Neither source is modified after landing. All cleaning happens downstream in Person 2's zones.
  Upload partitions are append-only: a new ingest always creates a new `run=<ts>` partition,
  and the skips-existing-ids rule means re-ingesting a file is a no-op rather than a rewrite.
- `logs/checksums.txt` (SHA-256) pins the exact Mercari archive + extracted TSV used.
- Generated data is reproducible from the seed for a given generator version; the run
  partition name in the path is the provenance marker.
- The canonical union reads the generated run pinned inside `spark_jobs/clean_normalize.py`
  (currently `run=2026_09_28T150245Z`). Console-trigger test batches may exist elsewhere in
  the generated zone; they are **not** part of the union — see `docs/SCHEMA_generated.md`.
