# ResellRadar Raw-Zone Validation Report

Generated: 2026-09-28 15:03:44Z  |  Runtime: 17.7s  |  Peak RSS: 204 MB

---

## Source 1: Mercari (REAL)

- **Rows**: 1,482,535

### Null rate per field

| Field | Null rate |
|---|---|
| train_id | 0.00% |
| name | 0.00% |
| item_condition_id | 0.00% |
| category_name | 0.43% |
| brand_name | 42.68% |
| price | 0.00% |
| shipping | 0.00% |
| item_description | 0.00% |

- Baseline check: `brand_name` null rate 42.68% (published ~43%, expected band 40%-47%) -> PASS
- Baseline check: `category_name` null rate 0.43% (published ~0.4%) -> PASS

### Duplicates

- `train_id` uniqueness (range check): PASS
- Zero-price rows: 874 (0.06%) - documented quality note
- `[rm]` removed descriptions: 63 (0.00%)

### Price distribution (USD)

- min $0.00 | p1 $3.00 | p25 $10.00 | median $17.00 | p75 $29.00 | p95 $75.00 | p99 $170.00 | max $2,009.00

### Category split (top level)

| Top category | Share |
|---|---|
| Women | 44.81% |
| Beauty | 14.02% |
| Kids | 11.58% |
| Electronics | 8.28% |
| Men | 6.32% |
| Home | 4.58% |
| Vintage & Collectibles | 3.14% |
| Other | 3.06% |
| Handmade | 2.08% |
| Sports & Outdoors | 1.71% |
| <MISSING> | 0.43% |

### Condition / shipping distribution

- condition 1-5: c1=43.21%, c2=25.33%, c3=29.15%, c4=2.16%, c5=0.16%
- shipping (1=seller pays): 0=55.27%, 1=44.73%

### Structural gate

- violations: 0 -> PASS

---

## Source 2: Generated (SYNTHETIC)

- **Rows**: 500,000 across 50 batch files
- Posted-date span: 2024-09-29 -> 2026-09-28 (729 days, target ~730)

### Null rate per field

| Field | Null rate | Target |
|---|---|---|
| listing_id | 0.00% | - |
| title | 0.00% | - |
| description | 5.05% | 3-7% |
| price | 1.97% | - |
| price_raw | 0.00% | - |
| currency | 0.00% | - |
| category | 0.00% | - |
| sub_category | 4.99% | 3-7% |
| category_full | 0.00% | - |
| item_condition_id | 0.00% | - |
| brand_name | 38.13% | - |
| shipping | 0.00% | - |
| seller_id | 0.00% | - |
| location_city | 0.00% | - |
| location_region | 0.00% | - |
| posted_date | 0.00% | - |
| delisted_date | 25.63% | - |
| seller_type | 0.00% | - |
| source_platform | 0.00% | - |

### Duplicates

- `listing_id` duplicates: 0
- Designed duplicate rate (same seller_id + normalized title within 14 days): **8.15%** (40,764 rows)
- Naive (title, price) duplicate rate: 1.15% (5,737 rows)

  > Note on the naive rate: at 500k rows over ~1,100 canonical products, 
> two independent listings can share (title, price) by chance - especially with 
> quantized price formats and a skewed product mix. The naive rate is therefore an 
> UPPER bound dominated by chance collisions, not the designed repost rate. 
> The designed repost share is verified against the ground-truth file below.

### Price distribution (USD)

- min $1.49 | p1 $17.06 | p25 $27.77 | median $43.96 | p75 $80.44 | p95 $141.17 | p99 $203.24 | max $3,423.69
- null prices (blank/negotiable): 1.97% (target ~2%)

### price_raw format distribution

| Format | Share |
|---|---|
| dollar | 34.34% |
| plain | 29.38% |
| k_suffix | 19.64% |
| dollar_slash | 14.67% |
| negotiable | 0.99% |
| blank | 0.98% |

### Category split

| Top category | Share |
|---|---|
| Electronics | 61.87% |
| Home | 38.13% |

### Messiness / signal markers

- Alias-city share (NYC/SF/LA/Philly): 19.00%
- `category_full` nulls: 0.00% (must be 0)

### Structural gate

- violations: 0 -> PASS
  - malformed ISO dates: 0
  - delisted < posted: 0
  - condition outside 1-5: 0
  - bad listing_id: 0

> Acceptance checks for embedded signal (depreciation, time-to-sale shape,
> regional spread, brand separation): run `python scripts/qa_generated.py --run-dir <run=partition>`.

---

## Union notes for Person 2

See `docs/SCHEMA_MISMATCHES.md`. Reminder: Mercari rows have NULL dates/location;
generated rows carry the full 17-column union schema. Duplicate rates are reported
per source and must never be pooled.