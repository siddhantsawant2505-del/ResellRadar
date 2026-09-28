# SCHEMA — Mismatches & Union Contract

**Purpose**: the single page Person 2 reads before touching the raw data. It defines how the
two sources union, what is missing where, and which source is real vs generated.

## Source status at a glance

| | Mercari `train.tsv` | Generated JSONL |
|---|---|---|
| **Status** | **REAL** (Kaggle marketplace dump) | **GENERATED** (synthetic, seeded; generator v2) |
| Rows | ~1,482,535 | ~500,000 |
| Landing zone | `data/raw/mercari/train.tsv` | `data/raw/generated/run=<ts>/gen_*_batchNNN.jsonl` |
| Format | TSV (header row) | JSON Lines, 10k records/file |
| Price | float USD (`0.0` rows exist) | float USD + messy `price_raw` string |
| Dates | **None** | posted/delisted, ISO dates, 24-month span |
| Location | **None** | city + region (US metros, aliases included) |
| Seller identity | **None** | `seller_id` (50k pool, Zipf-skewed) |

## Union schema (Person 2's `clean_normalize.py` target)

Read both sources with the columns below. Assign `source_platform`:
`"mercari"` for the real TSV (derived constant — Mercari never had this field),
`"generated"` is already present in the JSONL.

| Column | Mercari | Generated | Union rule |
|---|---|---|---|
| `listing_id` | `str(train_id)` | `listing_id` | cast Mercari ids to string; no overlap by construction (`GEN-` prefix vs digits) |
| `title` | `name` | `title` | direct |
| `description` | `item_description` (may be `[rm]` → treat as null) | nullable str | normalize both to null-or-str |
| `price` | float USD | float USD (null ~2%) | direct; **numeric in both by design** |
| `price_raw` | null (add column) | str | Mercari rows get NULL — there is no raw string |
| `currency` | add constant `"USD"` | `"USD"` | direct |
| `category` | split `category_name` part 1 (`Electronics`, `Home`, …) | `category` | generated uses Mercari top-level labels so no remap |
| `sub_category` | split `category_name` part 3 | nullable str | generated leaf matches Mercari leaf vocabulary |
| `category_full` | `category_name` (nullable) | `category_full` | exact pipe string on both sides |
| `item_condition_id` | int 1–5 | int 1–5 | direct (generated synthesizes the same scale) |
| `brand_name` | str, **~43% null** | str (never null) | **null-rate mismatch is real** — do not impute silently |
| `shipping` | 0/1 | 0/1 | direct |
| `posted_date` | **NULL for all rows** | ISO date | temporal analyses run on generated rows only (or documented synthetic enrichment) |
| `delisted_date` | **NULL for all rows** | nullable ISO date | same; null = still active |
| `location_city` | **NULL for all rows** | str | regional analyses run on generated rows only |
| `location_region` | **NULL for all rows** | str | same |
| `seller_type` | **NULL for all rows** | str | no ground truth in Mercari |
| `seller_id` | **NULL for all rows** (add column) | str `S-XXXXX` | **generated-only field**: 50k-seller pool with skew. Mercari has no seller identity — Person 2: add as nullable, never impute on Mercari rows. Duplicate detection on `seller_id` is generated-source only. |
| `source_platform` | constant `"mercari"` | constant `"generated"` | the only provenance flag — do not drop it |

## Hard mismatches (agreed handling)

1. **No dates/location in Mercari** — depreciation, resale velocity, and regional price
   variation are computed on the generated source. Brand/condition price effects can use the
   full 1.48M real rows. If the team wants time-series over the union, the only acceptable path
   is a documented *derived enrichment* (columns suffixed `_synthetic`), never silent.
2. **`price` nullability differs**: Mercari never null, generated null ~2% (messiness spec).
   Person 2: use nullable DoubleType for the union.
3. **`brand_name` nullability differs**: ~43% null (real) vs 0% (generated). Any model or
   aggregation on brand must report coverage per source.
4. **`sub_category` nullability differs**: ~0.4% missing `category_name` in Mercari vs 3–7%
   injected nulls in generated. `category_full` is never null in generated — use it for joins.
5. **`description` semantics**: Mercari has `[rm]` placeholders; generated has 3–7% true nulls.
6. **Duplicate semantics differ**: generated reposts (~10%, 8–12% band) are *within-source*
   engineered duplicates sharing `seller_id` with a near-identical title posted 1–14 days
   later; Mercari's residual dups are organic. Report duplicate rates per source, never pooled.
   Generated-side duplicates are defined as **same `seller_id` + normalized title within
   14 days**; the naive (title, price) collision rate is also reported and is dominated by
   chance collisions at this scale (see `logs/validation_report.md`).
7. **IDs are not comparable** — `listing_id` is a union-safe string key only, not a join key
   to anything external.
8. **Ground truth lives outside the raw zone**: `data/ground_truth/run=<ts>/truth.parquet`
   carries `listing_id, true_canonical_id, is_repost, original_listing_id`. It is Person 2's
   entity-resolution answer key — join on `listing_id` only, never merge into the raw zone,
   never use as a model feature. Raw JSONL contains no leakage columns by design.
9. **`seller_id` is generated-only**: there is no Mercari counterpart, so any seller-level
   aggregation must filter on `source_platform = 'generated'` (or run per-source as usual).

## Immutability & provenance

- Neither source is modified after landing. All cleaning happens downstream in Person 2's zones.
- `logs/checksums.txt` (SHA-256) pins the exact Mercari archive + extracted TSV used.
- Generated data is reproducible from the seed for a given generator version; the run
  partition name in the path is the provenance marker.
