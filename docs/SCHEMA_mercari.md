# SCHEMA — Mercari Source (REAL data)

**Source**: Kaggle *Mercari Price Suggestion Challenge*, `train.tsv`
**Status**: REAL marketplace data (label-verified below)
**Land zone**: `data/raw/mercari/train.tsv` — **immutable after extraction**
**Format**: TSV with header, UTF-8, one listing per line (~1,482,535 rows)

## Columns (exactly as shipped by Kaggle — never modified)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `train_id` | int64 | No | Row identifier (Mercari's primary key). |
| `name` | str | No | Listing title (short, marketplace-style). |
| `item_condition_id` | int | No | Condition 1 (worst) … 5 (best). |
| `category_name` | str | **Yes (~0.4%)** | Pipe-delimited `top/mid/leaf`, e.g. `Electronics/Cell Phones & Accessories/Cell Phones`. Missing category names exist (~6.3k rows). |
| `brand_name` | str | **Yes (~43%)** | Brand when listed, else missing. |
| `price` | float | No | Sale/list price in **USD**. Contains a small number of `0.0` rows (quality note, not parse errors). |
| `shipping` | int | No | `1` = shipping fee paid by seller, `0` = by buyer. |
| `item_description` | str | No* | Free text; some entries are just `[rm]` (removed by seller). |

## Fields Mercari does NOT have (documented, not fixable)

`posted_date`, `delisted_date`, `location_city`, `location_region`, `seller_type`,
`currency` (implicit USD), `source_platform` (derived constant `mercari` when unioned).

Any downstream table that joins Mercari with the generated source must add these as
explicit **NULL** columns for Mercari rows — see `docs/SCHEMA_MISMATCHES.md`.

## Known quality profile (use as validation baselines)

| Metric | Expected | Why it matters |
|---|---|---|
| Rows | ≈ 1,482,535 | Catches truncated/corrupt downloads. |
| `brand_name` null rate | ≈ 0.43 | Confirms correct parse of missing fields. |
| `category_name` null rate | ≈ 0.004 | Same. |
| `price` | float > 0 for ~99.5%; `0.0` rows exist | Catches dtype drift (str vs float). |
| `price` units | USD | Anchors the currency decision for the union. |

## Reproducibility

- `logs/checksums.txt` stores SHA-256 of the archive and of `train.tsv` after extraction.
- Re-download requires Kaggle credentials (`~/.kaggle/kaggle.json`) **and** prior
  rules acceptance of the competition on the Kaggle website.
