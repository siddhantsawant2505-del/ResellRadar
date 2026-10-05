import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    datediff,
    when,
    round,
    percentile_approx,
    stddev,
    min as spark_min,
    max as spark_max,
    months_between,
    floor
)
from pyspark.sql.window import Window


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

# Cluster batch mode: RR_DATA_ROOT (e.g. hdfs://namenode:9000/data) points input and
# outputs at the HDFS zones so the executors on the worker containers read/write the
# cluster storage. Default stays the local checkout (single-unit execution).
_DATA_ROOT = os.environ.get("RR_DATA_ROOT")

INPUT_PATH = (
    f"{_DATA_ROOT}/processed/entity_resolved.parquet"
    if _DATA_ROOT
    else str(
        BASE_DIR
        / "data"
        / "processed"
        / "entity_resolved.parquet"
    )
)

OUTPUT_DIR = BASE_DIR / "data" / "curated"


def curated_path(name):
    """Output path for a curated table (HDFS curated zone in cluster batch mode)."""
    if _DATA_ROOT:
        return f"{_DATA_ROOT}/curated/{name}"
    return str(OUTPUT_DIR / name)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-FeatureEngineering")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config(
        "spark.hadoop.io.native.lib.available",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# READ DATA
# ============================================================

print("Reading:")
print(INPUT_PATH)

df = spark.read.parquet(INPUT_PATH)

print("\n========== DATASET ==========")

total_records = df.count()

print("Total Records:", total_records)


# ============================================================
# RESALE DURATION
# ============================================================

df = df.withColumn(
    "resale_duration_days",
    when(
        col("delisted_date").isNotNull(),
        datediff(
            col("delisted_date"),
            col("posted_date")
        )
    )
)


# ============================================================
# DEPRECIATION CURVE
# ============================================================

print("\n========== DEPRECIATION CURVE ==========")


# Find the first observed listing date for every entity
entity_window = Window.partitionBy("entity_id")

df = df.withColumn(
    "entity_first_posted_date",
    spark_min("posted_date").over(entity_window)
)


# Calculate listing age in months
df = df.withColumn(
    "listing_age_months",
    floor(
        months_between(
            col("posted_date"),
            col("entity_first_posted_date")
        )
    )
)


# Prevent negative values
df = df.withColumn(
    "listing_age_months",
    when(
        col("listing_age_months") < 0,
        0
    ).otherwise(
        col("listing_age_months")
    )
)


# Aggregate price by entity and listing age
depreciation_curve = df.groupBy(
    "entity_id",
    "listing_age_months"
).agg(
    count("*").alias(
        "listing_count"
    ),
    round(
        avg("price"),
        2
    ).alias(
        "average_price"
    ),
    round(
        percentile_approx(
            "price",
            0.5
        ),
        2
    ).alias(
        "median_price"
    )
)


# ============================================================
# CORRECT BASELINE PRICE
# ============================================================

# Baseline = average price at age 0
baseline_prices = depreciation_curve.filter(
    col("listing_age_months") == 0
).select(
    "entity_id",
    col("average_price").alias(
        "baseline_price"
    )
)


# Join baseline price back to every age group
depreciation_curve = depreciation_curve.join(
    baseline_prices,
    on="entity_id",
    how="left"
)


# ============================================================
# PRICE CHANGE %
# ============================================================

depreciation_curve = depreciation_curve.withColumn(
    "price_change_percent",
    when(
        col("baseline_price") > 0,
        round(
            (
                (
                    col("average_price")
                    - col("baseline_price")
                )
                / col("baseline_price")
            ) * 100,
            2
        )
    ).otherwise(
        None
    )
)


# Sort output
depreciation_curve = depreciation_curve.orderBy(
    "entity_id",
    "listing_age_months"
)


print("Depreciation curve created.")

depreciation_curve.show(
    20,
    truncate=False
)


# ============================================================
# RESALE VELOCITY
# ============================================================

print("\n========== RESALE VELOCITY ==========")

velocity = df.filter(
    col("resale_duration_days").isNotNull()
).groupBy(
    "entity_id"
).agg(
    count("*").alias(
        "delisted_listings"
    ),
    round(
        avg("resale_duration_days"),
        2
    ).alias(
        "avg_resale_days"
    ),
    round(
        percentile_approx(
            "resale_duration_days",
            0.5
        ),
        2
    ).alias(
        "median_resale_days"
    )
)


velocity = velocity.orderBy(
    col("avg_resale_days").asc()
)


print("Resale velocity table created.")

velocity.show(
    20,
    truncate=False
)


# ============================================================
# REGIONAL PRICE VARIANCE
# ============================================================

print("\n========== REGIONAL PRICE VARIANCE ==========")

regional = df.groupBy(
    "entity_id",
    "location_region"
).agg(
    count("*").alias(
        "listing_count"
    ),
    round(
        avg("price"),
        2
    ).alias(
        "average_price"
    ),
    round(
        stddev("price"),
        2
    ).alias(
        "price_stddev"
    )
)


regional_window = Window.partitionBy(
    "entity_id"
)


# Minimum regional average price
regional = regional.withColumn(
    "min_regional_price",
    round(
        spark_min(
            "average_price"
        ).over(regional_window),
        2
    )
)


# Maximum regional average price
regional = regional.withColumn(
    "max_regional_price",
    round(
        spark_max(
            "average_price"
        ).over(regional_window),
        2
    )
)


# Regional price range
regional = regional.withColumn(
    "regional_price_range",
    round(
        col("max_regional_price")
        - col("min_regional_price"),
        2
    )
)


print("Regional price variance table created.")

regional.show(
    20,
    truncate=False
)


# ============================================================
# SAVE CURATED DATA
# ============================================================

print("\n========== SAVING CURATED DATA ==========")


if not _DATA_ROOT:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )


# Depreciation curve
depreciation_curve.write.mode(
    "overwrite"
).parquet(
    curated_path(
        "depreciation_curve_curated.parquet"
    )
)


# Resale velocity
velocity.write.mode(
    "overwrite"
).parquet(
    curated_path(
        "resale_velocity_curated.parquet"
    )
)


# Regional price variance
regional.write.mode(
    "overwrite"
).parquet(
    curated_path(
        "regional_price_variance_curated.parquet"
    )
)


# ============================================================
# SUCCESS
# ============================================================


print(
    "\nSUCCESS!"
)

print(
    "Depreciation curve saved to:"
)

print(
    curated_path(
        "depreciation_curve_curated.parquet"
    )
)

print(
    "\nResale velocity saved to:"
)

print(
    curated_path(
        "resale_velocity_curated.parquet"
    )
)

print(
    "\nRegional price variance saved to:"
)

print(
    curated_path(
        "regional_price_variance_curated.parquet"
    )
)


spark.stop()