# ResellRadar ER evaluation

Truth: `data\ground_truth\run=2026_09_28T150245Z\truth.parquet`  |  Predictions: `data/processed/entity_resolved.parquet`

## Coverage & integrity

- predictions rows: 1,972,679 (truth 500,000)
- duplicate listing_ids in predictions: 0
- truth listings with no prediction: 9,856 (coverage 98.03%)

## Task A - canonical clustering (pairwise)

- true same-canonical pairs : 178,104,985
- predicted same-cluster pairs: 171,178,690
- correct pairs (intersection): 171,178,690
- precision 1.0000 | recall 0.9611 | **F1 0.9802**

## Task B - repost detection

- source of repost flag: predicted_is_repost column
- truth reposts 50,000 | flagged 49,788 | hits 49,027
- precision 0.9847 | recall 0.9805 | **F1 0.9826**

## Task C - repost -> original link recovery

- truth reposts: 50,000 | links predicted: 49,029 | exact matches: 47,636
- precision 0.9716 | recall 0.9527 | **F1 0.9621**
