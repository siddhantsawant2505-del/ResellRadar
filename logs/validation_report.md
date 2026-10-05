# ResellRadar Raw-Zone Validation Report

Generated: 2026-10-05 18:12:35Z  |  Runtime: 13.9s  |  Peak RSS: 163 MB

---

---

## Source 2: Generated (SYNTHETIC)

- **Rows**: 551,500 across 58 batch files
- Posted-date span: 2024-09-29 -> 2026-10-05 (736 days, target ~730)

### Null rate per field

| Field | Null rate | Target |
|---|---|---|
| listing_id | 0.00% | - |
| title | 0.00% | - |
| description | 5.04% | 3-7% |
| price | 1.98% | - |
| price_raw | 0.00% | - |
| currency | 0.00% | - |
| category | 0.00% | - |
| sub_category | 4.98% | 3-7% |
| category_full | 0.00% | - |
| item_condition_id | 0.00% | - |
| brand_name | 38.13% | - |
| shipping | 0.00% | - |
| seller_id | 0.00% | - |
| location_city | 0.00% | - |
| location_region | 0.00% | - |
| posted_date | 0.00% | - |
| delisted_date | 25.67% | - |
| seller_type | 0.00% | - |
| source_platform | 0.00% | - |

### Duplicates

- `listing_id` duplicates: 51,500
- Designed duplicate rate (same seller_id + normalized title within 14 days): **16.65%** (91,837 rows)
- Naive (title, price) duplicate rate: 10.29% (56,751 rows)

  > Note on the naive rate: at 500k rows over ~1,100 canonical products, 
> two independent listings can share (title, price) by chance - especially with 
> quantized price formats and a skewed product mix. The naive rate is therefore an 
> UPPER bound dominated by chance collisions, not the designed repost rate. 
> The designed repost share is verified against the ground-truth file below.

### Price distribution (USD)

- min $1.49 | p1 $17.05 | p25 $27.78 | median $43.97 | p75 $80.46 | p95 $141.18 | p99 $203.43 | max $3,423.69
- null prices (blank/negotiable): 1.98% (target ~2%)

### price_raw format distribution

| Format | Share |
|---|---|
| dollar | 34.32% |
| plain | 29.39% |
| k_suffix | 19.62% |
| dollar_slash | 14.68% |
| blank | 0.99% |
| negotiable | 0.98% |

### Category split

| Top category | Share |
|---|---|
| Electronics | 61.87% |
| Home | 38.13% |

### Messiness / signal markers

- Alias-city share (NYC/SF/LA/Philly): 18.98%
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