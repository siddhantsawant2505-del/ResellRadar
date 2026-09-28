# =====================================================================
# ACCEPTANCE COPY - Person 1 (2026-09-29)
#
# Byte-identical to spark_jobs/entity_resolution.py EXCEPT for one bounded block, so the
# uploaded job can be executed and graded. `diff spark_jobs/entity_resolution.py
# scripts/entity_resolution_lsh_bounded.py` shows the whole change.
#
# WHY: the `pairs` DataFrame built by approxSimilarityJoin is never consumed -- entity_id
# comes from row_number() over distinct model_key (see MODEL -> ENTITY below), and `pairs`
# only feeds a 30-row .show() of similar titles. On the delivered data the model set is
# 1,170,504 rows, so that display-only self-join explodes to 17,360,494,325 candidate
# pairs (measured: logs/diag_lsh_candidates.log) and never finishes. Sampling the demo
# input leaves the clustering output identical (the sample is not a clustering input).
# =====================================================================

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    array_distinct,
    row_number,
    regexp_replace,
    trim,
    when,
    lag,
    datediff,
    first_value
)
from pyspark.sql.window import Window
from pyspark.ml.feature import HashingTF, MinHashLSH


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_PATH = str(
    BASE_DIR / "data" / "processed" / "clean_listings.parquet"
)

OUTPUT_PATH = str(
    BASE_DIR / "data" / "processed" / "entity_resolved.parquet"
)


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-EntityResolution")
    .master("local[*]")
    .config(
        "spark.hadoop.io.native.lib.available",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# READ CLEAN DATA
# ============================================================

print("Reading:")
print(INPUT_PATH)

df = spark.read.parquet(INPUT_PATH)

total_records = df.count()

print("\n========== DATASET ==========")
print("Total Records:", total_records)


# ============================================================
# CREATE MODEL-AWARE KEY
# ============================================================

df = df.withColumn(
    "model_key",
    col("title_clean")
)

# Remove storage sizes
df = df.withColumn(
    "model_key",
    regexp_replace(
        col("model_key"),
        r"\b(16|32|64|128|256|512)\s*gb\b",
        ""
    )
)

df = df.withColumn(
    "model_key",
    regexp_replace(
        col("model_key"),
        r"\b(1|2)\s*tb\b",
        ""
    )
)

# Remove common colors
df = df.withColumn(
    "model_key",
    regexp_replace(
        col("model_key"),
        r"\b(black|white|blue|red|green|yellow|purple|pink|gray|grey|"
        r"silver|gold|snow|sea|charcoal|natural|tan|brown|beige)\b",
        ""
    )
)

# Remove common listing / condition phrases
df = df.withColumn(
    "model_key",
    regexp_replace(
        col("model_key"),
        r"\b(like new|mint condition|excellent condition|good condition|"
        r"fair condition|great condition|fully functional|minor scuffs|"
        r"with box|original box|unlocked|battery health)\b",
        ""
    )
)

# Clean spaces
df = df.withColumn(
    "model_key",
    trim(
        regexp_replace(
            col("model_key"),
            r"\s+",
            " "
        )
    )
)


# ============================================================
# PROTECT PRO / PRO MAX
# ============================================================

df = df.withColumn(
    "model_key",
    when(
        col("model_key").contains("pro max"),
        regexp_replace(
            col("model_key"),
            r"\bpro max\b",
            "promax"
        )
    ).otherwise(
        col("model_key")
    )
)


# ============================================================
# UNIQUE MODEL KEYS
# ============================================================

unique_models = df.select(
    "title_clean",
    "model_key"
).distinct()

print("\n========== UNIQUE MODELS ==========")

print(
    "Unique model keys:",
    unique_models.select("model_key").distinct().count()
)


# ============================================================
# TOKENIZE
# ============================================================

model_key_df = unique_models.withColumn(
    "tokens",
    array_distinct(
        split(col("model_key"), " ")
    )
)


# ============================================================
# HASHING
# ============================================================

hashing_tf = HashingTF(
    inputCol="tokens",
    outputCol="features",
    numFeatures=4096
)

model_key_df = hashing_tf.transform(model_key_df)


# ============================================================
# MINHASH LSH
# ============================================================

mh = MinHashLSH(
    inputCol="features",
    outputCol="hashes",
    numHashTables=5
)

model = mh.fit(model_key_df)

print("\n========== MINHASH LSH ==========")
print("MinHash model created successfully.")


# ============================================================
# SIMILAR MODEL CANDIDATES
# ============================================================

# Acceptance patch: this is a DISPLAY-ONLY demo (see the header). Sample its input so the
# block is affordable; the clustering below does not read `pairs`.
demo_df = model_key_df.sample(
    fraction=0.02,
    seed=42
)

pairs = model.approxSimilarityJoin(
    demo_df,
    demo_df,
    0.3,
    distCol="jaccard_distance"
)

pairs = pairs.filter(
    col("datasetA.title_clean") <
    col("datasetB.title_clean")
)


# ============================================================
# PRO / PRO MAX PROTECTION
# ============================================================

pairs = pairs.filter(
    ~(
        col("datasetA.model_key").contains("pro ")
        &
        col("datasetB.model_key").contains("promax")
    )
)

pairs = pairs.filter(
    ~(
        col("datasetA.model_key").contains("promax")
        &
        col("datasetB.model_key").contains("pro ")
    )
)


print("\n========== SIMILAR MODEL PAIRS ==========")

pairs.select(
    col("datasetA.title_clean").alias("title_a"),
    col("datasetB.title_clean").alias("title_b"),
    col("datasetA.model_key").alias("model_a"),
    col("datasetB.model_key").alias("model_b"),
    col("jaccard_distance")
).orderBy(
    col("jaccard_distance").asc()
).show(
    30,
    truncate=False
)


# ============================================================
# CREATE ENTITY IDS
# ============================================================

window = Window.orderBy("model_key")

model_entities = (
    model_key_df
    .select("model_key")
    .distinct()
    .withColumn(
        "entity_id",
        row_number().over(window)
    )
)


# ============================================================
# MODEL -> ENTITY
# ============================================================

model_mapping = model_entities.select(
    "model_key",
    "entity_id"
)


# ============================================================
# TITLE -> ENTITY
# ============================================================

title_mapping = (
    model_key_df
    .select(
        "title_clean",
        "model_key"
    )
    .join(
        model_mapping,
        on="model_key",
        how="left"
    )
    .select(
        "title_clean",
        "entity_id"
    )
)


# ============================================================
# MAP ENTITIES TO LISTINGS
# ============================================================

df = df.join(
    title_mapping,
    on="title_clean",
    how="left"
)


# ============================================================
# CREATE REPOST TITLE KEY
# ============================================================

# Remove known repost suffixes only for repost detection.
# Original title_clean remains unchanged.

df = df.withColumn(
    "repost_title_key",
    regexp_replace(
        col("title_clean"),
        r"\s*(must go|relist)\s*$",
        ""
    )
)

df = df.withColumn(
    "repost_title_key",
    regexp_replace(
        col("repost_title_key"),
        r"\s*!+\s*$",
        ""
    )
)

df = df.withColumn(
    "repost_title_key",
    trim(col("repost_title_key"))
)


# ============================================================
# REPOST DETECTION
# ============================================================

print("\n========== REPOST DETECTION ==========")

# Repost requires:
# - same seller
# - same normalized / near-identical title
# - 1 to 14 days after previous listing
#
# Ground truth is NOT used.

repost_window = (
    Window
    .partitionBy(
        "seller_id",
        "repost_title_key"
    )
    .orderBy(
        "posted_date"
    )
)

df = df.withColumn(
    "previous_posted_date",
    lag("posted_date").over(repost_window)
)

df = df.withColumn(
    "days_since_previous",
    datediff(
        col("posted_date"),
        col("previous_posted_date")
    )
)

df = df.withColumn(
    "predicted_is_repost",
    when(
        col("seller_id").isNotNull()
        &
        col("posted_date").isNotNull()
        &
        col("previous_posted_date").isNotNull()
        &
        (col("days_since_previous") >= 1)
        &
        (col("days_since_previous") <= 14),
        1
    ).otherwise(0)
)


# ============================================================
# ORIGINAL LISTING
# ============================================================

original_window = (
    Window
    .partitionBy(
        "seller_id",
        "repost_title_key"
    )
    .orderBy(
        "posted_date"
    )
)

df = df.withColumn(
    "predicted_original_listing_id",
    first_value("listing_id").over(
        original_window
    )
)


# ============================================================
# RESULTS
# ============================================================

print(
    "Detected reposts:",
    df.filter(
        col("predicted_is_repost") == 1
    ).count()
)

print(
    "Unique Entities:",
    df.select("entity_id").distinct().count()
)

print(
    "Listings with Entity ID:",
    df.filter(
        col("entity_id").isNotNull()
    ).count()
)


# ============================================================
# REMOVE TEMPORARY COLUMNS
# ============================================================

df = df.drop(
    "model_key",
    "repost_title_key",
    "previous_posted_date",
    "days_since_previous"
)


# ============================================================
# SAVE OUTPUT
# ============================================================

print("\n========== SAVING OUTPUT ==========")

df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_PATH
)


print("\nSUCCESS!")

print(
    "Entity-resolved dataset saved to:"
)

print(
    OUTPUT_PATH
)


spark.stop()