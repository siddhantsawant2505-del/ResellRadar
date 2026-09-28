# ResellRadar pipeline acceptance checks

## stage1

- [PASS] row count equals raw 1,982,535 minus the 9,856 designed null-price rows -- 1,972,679 rows (expected 1,972,679)
- [PASS] no duplicate listing_id after dropDuplicates -- 0 duplicates
- [PASS] listing_id / title / price / source_platform are never null -- {'listing_id': 0, 'title': 0, 'price': 0, 'source_platform': 0}
- [PASS] both sources survived the union with the expected row counts -- mercari=1,482,535 (expected 1,482,535), generated=490,144 (expected 490,144)
- [PASS] Mercari rows carry NULL dates/location/seller_id (no such fields in real data) -- posted_date=1,482,535, delisted_date=1,482,535, seller_id=1,482,535, location_city=1,482,535
- [PASS] generated rows keep their dates and seller_id -- posted_date nulls=0, seller_id nulls=0
- [PASS] title_clean is lowercased/alphanumeric-only for every row -- 1,972,679 non-null of 1,972,679
- [PASS] generated rows always have category_full (never null by design) -- 0 nulls in generated rows
- [PASS] Mercari category_full nulls stay within the documented ~0.4% missing category_name -- 6,327 nulls (0.43%) in mercari rows

## stage2

- [PASS] all required columns present (union schema + ER outputs) -- 24 columns
- [PASS] temporary ER helper columns dropped before writing -- []
- [PASS] row count unchanged from stage 1 (ER must not drop listings) -- 1,972,679 rows (expected 1,972,679)
- [PASS] no duplicate listing_id -- 0 duplicates
- [PASS] every listing received an entity_id (title -> entity mapping is total) -- 1,065,540 distinct entities, 0 nulls
- [PASS] predicted_is_repost is a 0/1 flag -- values=[0, 1]
- [PASS] flagged reposts all carry a link to an earlier original -- 49,788 of 49,788 flagged rows linked
- [PASS] a repost never links to itself -- 0 self-links
- [PASS] every predicted original id exists in the dataset -- 0 dangling links

## stage3

- [PASS] depreciation_curve_curated.parquet written and non-empty -- 1,087,764 rows
- [PASS] depreciation_curve_curated.parquet has no entirely-null column -- []
- [PASS] resale_velocity_curated.parquet written and non-empty -- 1,105 rows
- [PASS] resale_velocity_curated.parquet has no entirely-null column -- []
- [PASS] regional_price_variance_curated.parquet written and non-empty -- 1,080,928 rows
- [PASS] regional_price_variance_curated.parquet has no entirely-null column -- []

