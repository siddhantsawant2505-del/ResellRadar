# SCHEMA — Generated Source (SYNTHETIC data)

**Status**: GENERATED (synthetic, generator v2) — produced by `generator/generate_data.py`
**Land zone**: `data/raw/generated/run=<UTC ts>/gen_YYYY_MM_DD_batchNNN.jsonl` — **immutable after write**
**Format**: JSON Lines, UTF-8, 10,000 records per file
**Reproducibility**: fixed RNG seed (`SEED = 42` in the generator); batches are deterministic
for a given generator version. Files are never overwritten — a new code version means a new run partition.

This source exists to supply what Mercari lacks (dates, location) and to carry the
required messiness. Products, brands, and price levels are seeded from the real
Mercari phone/furniture sample (`data/seed/mercari_phone_furniture_sample.csv`).

## Fields (19)

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `listing_id` | str | No | `GEN-{batchNNN}-{seq:06d}`; unique across the run. |
| `title` | str | No | 6–15 surface variants per canonical product. |
| `description` | str | **Yes** | 3–7% null (required messiness target). |
| `price` | float | **Yes** | Numeric USD. Null iff `price_raw` is blank/"negotiable" (~2% of rows). |
| `price_raw` | str | No | The messy original: `"$15,000"`, `"15k"`, `"$15,000/-"`, `"15000"`, `"negotiable"`, `""`. Person 2's parsing exercise. |
| `currency` | str | No | Constant `"USD"` (locked team decision). |
| `category` | str | No | Mercari top-level: `Electronics` or `Home`. |
| `sub_category` | str | **Yes** | Mercari leaf, e.g. `Cell Phones`, `Sofas & Couches`; 3–7% null (messiness target). |
| `category_full` | str | No | Exact Mercari-style pipe string, e.g. `Electronics/Cell Phones & Accessories/Cell Phones` — join key to the real source. Never nulled even when `sub_category` is. |
| `location_city` | str | No | US metro incl. aliases: `NYC`, `New York`, `SF`, `San Francisco`, `LA`, `Los Angeles`, `Philly`, `Philadelphia`, `Austin`, … |
| `location_region` | str | No | Two-letter state (`NY`, `CA`, `TX`, …) — always the canonical state even when the city is an alias. |
| `posted_date` | str | No | ISO date `YYYY-MM-DD`, spread over 24 months. |
| `delisted_date` | str | **Yes** | ISO date; null = still active. Nulls = active share + 3–7% missingness target. Drawn from a log-normal time-to-sale; always ≥ `posted_date`. |
| `seller_type` | str | No | `Individual` (dominant), `PowerSeller`, `Refurbisher`, `Liquidator`. |
| `source_platform` | str | No | Constant `"generated"`. |
| `item_condition_id` | int | No | 1–5, Mirari-compatible condition scale (synthesized). |
| `brand_name` | str | No | Sampled from real Mercari phone/furniture brands. (Never null here; Mercari is ~43% null — see mismatches doc.) |
| `shipping` | int | No | 0/1, Mercari-compatible. |
| `seller_id` | str | No | `S-XXXXX` from a 50,000-seller pool with Zipf-like skew (s = 0.8): power sellers post thousands of listings, the long tail posts few. **Generated-only** — Mercari has no seller field (see mismatches doc). |

## Canonical catalog (1,000+ products)

The generator samples from **1,108 canonical products** built by cross-product:

- **Phones (688)**: brand x model x storage x 8-color palette
  (`P:Apple|iPhone 13|128GB|Midnight Black`).
- **Furniture (420)**: leaf x type x 5 materials x 3 sizes
  (`F:Living Room Furniture|Leather Sofa|Metal|Compact`).

Each canonical has a deterministic 6–15 title-variant template pool. Brand/leaf sampling
weights come from the real Mercari calibration; brands below the seed-sample threshold
(Google, Motorola, Xiaomi) get a small synthetic share (2.5% total, fallback-weighted)
so every canonical is realizable.

The canonical id itself is **never written to the raw JSONL** — it lives only in the
ground-truth file below.

## Ground truth (OUTSIDE the raw zone)

`data/ground_truth/run=<same ts>/truth.parquet` — one row per raw listing:

| Column | Type | Notes |
|---|---|---|
| `listing_id` | str | 1:1 join key to the raw JSONL `listing_id`. |
| `true_canonical_id` | str | The canonical product this listing depicts. |
| `is_repost` | bool | True for designed reposts (~10% of rows). |
| `original_listing_id` | str/null | The original listing this repost re-lists; null for originals. Links never dangle. |

This file is Person 2's **entity-resolution answer key** and is kept out of the raw zone
by design (raw JSONL stays free of leakage columns). It is gitignored and pinned in
`logs/checksums.txt`.

## Embedded signal (the analytical payload — Person 2/3 depend on these)

- **Depreciation**: price declines with item age + multiplicative noise.
- **Time-to-sale**: log-normal distribution; `delisted_date - posted_date` follows it.
- **Regional price effects**: per-city multipliers create real regional variation.
- **Reposts**: ~10% designed near-duplicates (8–12% band) — same `seller_id` as the
  original, near-identical title (original + small suffix), posted 1–14 days later,
  price perturbed ±5%. The realistic duplicate population for entity resolution; the
  `(is_repost, original_listing_id)` link lives only in the ground-truth file.

## Messiness targets (validated by `scripts/validate_raw.py`)

| Target | Spec | Report section |
|---|---|---|
| Title variants | 6–15 per canonical product | variant-count histogram |
| Price formats | ≥5 formats incl. `15k`, `/$/-` suffixes | `price_raw` format distribution |
| Blank/negotiable | ~2% (price null) | null rate of `price` |
| Near-duplicate reposts | ~10% (8–12% band), same seller_id + near-identical title, 1–14d later | designed dup rate (seller_id + normalized title within 14d); naive (title, price) rate reported separately with chance-collision note |
| City aliases | NYC/SF/LA/Philly vs canonical | alias vs canonical ratio |
| Nulls | 3–7% in `description`, `sub_category` (+null `delisted_date`) | per-field null rates |
| Outlier prices | ~1% extreme/bait (×0.1 or ×8) | outlier count |
| Date span | 24 months | min/max posted_date |
