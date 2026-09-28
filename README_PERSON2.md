# ResellRadar — Person 2: Big Data Processing

## Role

**Person 2 — Big Data Processing Engineer**

Responsible for transforming the raw ResellRadar listing data into clean, entity-resolved, and analytics-ready datasets using Apache Spark.

---

## Objectives

The Big Data Processing pipeline performs:

1. Schema normalization
2. Data cleaning and text preprocessing
3. Duplicate handling
4. Entity resolution
5. Repost detection
6. Feature engineering
7. Generation of curated analytics datasets

---

# Pipeline

```text
Raw Data
   ↓
Schema Normalization
   ↓
Data Cleaning
   ↓
Duplicate Removal
   ↓
Entity Resolution
   ↓
Repost Detection
   ↓
Feature Engineering
   ↓
Curated Analytics Datasets
```

---

# 1. Data Cleaning & Normalization

### File

```text
spark_jobs/clean_normalize.py
```

### Input

The pipeline combines data from:

* Mercari dataset
* Generated ResellRadar listings

### Common Schema

The datasets are normalized into a common 19-field schema:

```text
listing_id
title
description
price
price_raw
currency
category
sub_category
category_full
item_condition_id
brand_name
shipping
posted_date
delisted_date
location_city
location_region
seller_type
seller_id
source_platform
```

Additional processing columns are created later in the pipeline.

### Cleaning Operations

* Standardized column names and data types
* Converted prices to numeric values
* Normalized category fields
* Converted invalid description markers such as `[rm]` to null
* Created cleaned title and description fields
* Removed records missing essential fields
* Removed duplicate listing IDs

### Result

```text
Raw records:       1,982,535
Clean records:     1,972,679
Records removed:       9,856
Duplicates removed:        0
```

Output:

```text
data/processed/clean_listings.parquet
```

---

# 2. Entity Resolution

### File

```text
spark_jobs/entity_resolution.py
```

Entity resolution identifies listings that potentially represent the same underlying product/model.

### Processing

The pipeline:

* Normalizes product titles
* Removes irrelevant attributes such as storage sizes, colors and condition phrases
* Protects important model distinctions such as `Pro` vs `Pro Max`
* Generates normalized model keys
* Uses MinHash LSH to identify similar model keys
* Generates deterministic entity IDs
* Creates repost-related keys
* Detects potential reposts based on seller, title key and posting dates

### Output

```text
data/processed/entity_resolved.parquet
```

### Results

```text
Input records:       1,972,679
Output records:      1,972,679
Unique entities:     1,102,295
Detected reposts:       49,808
```

No records were lost during entity resolution.

---

# 3. Feature Engineering

### File

```text
spark_jobs/feature_engineering.py
```

The feature engineering stage generates aggregated analytics datasets for the dashboard.

---

## 3.1 Depreciation Curve

Groups listings by:

```text
category
posted_year
```

Generated metrics:

* Listing count
* Average price
* Minimum price
* Maximum price

Output:

```text
data/curated/depreciation_curve_curated.parquet
```

Current output:

```text
6 aggregated records
```

The current dataset contains:

* Electronics
* Home

across the available years.

---

## 3.2 Resale Velocity

Calculates the time between:

```text
posted_date → delisted_date
```

Only valid resale intervals between 0 and 3650 days are included.

Generated metrics:

* Resale count
* Average days to resale
* Median days to resale
* Minimum days to resale
* Maximum days to resale

Output:

```text
data/curated/resale_velocity_curated.parquet
```

Current output:

```text
2 aggregated records
```

---

## 3.3 Regional Price Variance

Groups listings by:

```text
location_region
category
```

Generated metrics:

* Listing count
* Average price
* Price standard deviation
* Price variance
* Minimum price
* Maximum price

Output:

```text
data/curated/regional_price_variance_curated.parquet
```

Current output:

```text
34 aggregated records
```

---

# Final Dataset Outputs

| Dataset                                   | Purpose                            |
| ----------------------------------------- | ---------------------------------- |
| `clean_listings.parquet`                  | Clean normalized listing data      |
| `entity_resolved.parquet`                 | Entity-resolved listing-level data |
| `depreciation_curve_curated.parquet`      | Depreciation analytics             |
| `resale_velocity_curated.parquet`         | Resale speed analytics             |
| `regional_price_variance_curated.parquet` | Regional pricing analytics         |

---

# Dataset Sharing

The generated Parquet datasets are **not stored in GitHub** because of their size.

They are stored in the team's shared Google Drive:

```text
data/
├── processed/
│   └── entity_resolved.parquet
│
└── curated/
    ├── depreciation_curve_curated.parquet
    ├── resale_velocity_curated.parquet
    └── regional_price_variance_curated.parquet
```

The processing scripts are available in GitHub.

---

# GitHub Branch

Person 2 development branch:

```text
feature/big-data-processing
```

Main processing files:

```text
spark_jobs/
├── clean_normalize.py
├── entity_resolution.py
└── feature_engineering.py
```

---

# Running the Pipeline

From the project root:

### Stage 1

```powershell
python spark_jobs\clean_normalize.py
```

### Stage 2

```powershell
python spark_jobs\entity_resolution.py
```

### Stage 3

```powershell
python spark_jobs\feature_engineering.py
```

The scripts are configured for the project's Windows + PySpark environment and include the required Hadoop configuration.

---

# Final Processing Summary

```text
Raw Records
1,982,535
       ↓
Clean Records
1,972,679
       ↓
Entity-Resolved Records
1,972,679
       ↓
Unique Entities
1,102,295
       ↓
Detected Reposts
49,808
       ↓
Curated Analytics
├── Depreciation Curve
├── Resale Velocity
└── Regional Price Variance
```

---

# Handoff to Person 3

Person 3 should:

1. Pull the `feature/big-data-processing` branch.
2. Obtain the Parquet datasets from the shared Google Drive.
3. Keep the existing `data/processed/` and `data/curated/` folder structure.
4. Use `entity_resolved.parquet` for listing-level analytics.
5. Use the three curated datasets for dashboard visualizations.
6. Avoid modifying the original processed datasets.
7. Create separate derived datasets/views if additional dashboard-specific transformations are required.

The Big Data Processing stage is complete and ready for dashboard/API integration.

