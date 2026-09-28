"""
ResellRadar - Stage 2 entity resolution, v2 (Person 1 fix for the delivered job).

Reads :  data/processed/clean_listings.parquet
Writes:  data/processed/entity_resolved.parquet   (same schema/path as v1, so Stage 3 and
                                                   the dashboard need no change)

What changed vs spark_jobs/entity_resolution.py
-----------------------------------------------
1. ENTITY KEY = PRODUCT TOKENS, NOT A DESTRUCTIVELY STRIPPED STRING.
   v1 stripped storage sizes and colours ("128gb", "black", "titanium", ...) and then keyed
   the entity on whatever was left. But docs/SCHEMA_generated.md defines a canonical product
   as `brand x model x storage x colour` (phones) / `type x material x size` (furniture) and
   a title as a *surface variant* of it ("6-15 surface variants per canonical product"). So
   storage and colour ARE the product identity; removing them splits one product into ~39
   keys (only 3 of 1,107 products were covered by a single v1 key) and merges different
   products that happen to share the remainder.
   The real surface noise is the documented chatter vocabulary: condition words
   (COND_WORDS), TITLE_EXTRAS, the "Used"/"FS:"/"Selling" prefixes, the
   "- {city} pickup" suffix (stripped using each row's own location_city) and the repost
   suffixes. Removing exactly that and keeping everything else recovers the product.
   Order-insensitive (sorted token set) so the generator's "last-token-first" variant
   collapses too.

2. THE DEAD MinHash LSH SELF-JOIN IS GONE.
   In v1 the `pairs` DataFrame was filtered and `show(30)`n but never consumed - entity_id
   came from row_number() over distinct model_key, so the LSH contributed nothing while
   costing 17,360,494,325 candidate pairs on the delivered data (measured:
   logs/diag_lsh_candidates.txt), which is why v1 never finished. Removing it also removes
   the pyspark.ml / numpy dependency.

3. NO TITLE INDIRECTION.
   v1 mapped title -> entity and joined listings on title_clean, which silently duplicates
   rows if one title ever maps to two entities. The key is a per-row function, so listings
   join the entity table directly on model_key.

Everything else (repost detection, the output schema, the written path) is unchanged from
v1, so repost scoring is unaffected.

Acceptance evidence: logs/acceptance_2026_09_29.md, logs/er_report_stage2_v2.md.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    trim,
    lower,
    coalesce,
    lit,
    concat_ws,
    array_distinct,
    array_except,
    sort_array,
    size,
    row_number,
    regexp_replace,
    when,
    lag,
    datediff,
    first_value,
)
from pyspark.sql.window import Window


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
# CHATTER VOCABULARY (documented messiness spec)
# ============================================================
# generator/generate_data.py: COND_WORDS + TITLE_EXTRAS + the "Used"/"FS:"/"Selling"
# prefixes, the "- {city} pickup" suffix and REPOST_SUFFIXES. These words are listing
# chatter; they never identify a product. Everything NOT listed here is kept.

CONDITION_WORDS = ["for", "parts", "poor", "fair", "good", "like", "new"]
TITLE_EXTRAS = ["with", "box", "clean", "imei", "no", "scratches", "screen",
                "protector", "on", "factory", "unlocked", "esim", "ready"]
TEMPLATE_FILLERS = ["used", "fs", "selling", "pickup", "must", "go", "relist"]

CHATTER = sorted(set(CONDITION_WORDS + TITLE_EXTRAS + TEMPLATE_FILLERS))


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-EntityResolution-v2")
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

print("\n========== DATASET ==========")
print("Total Records:", df.count())


# ============================================================
# ENTITY KEY
# ============================================================
# title tokens, minus the listing's own city, minus the chatter vocabulary.
# array_except is a set operation, so tokens are de-duplicated and order-insensitive:
# "titanium apple iphone 13 128gb titanium" -> {13, 128gb, apple, iphone, titanium}.

# clean_normalize strips punctuation but keeps the spaces around it, so the " - Unlocked"
# / " - {city} pickup" / " - {condition}" templates leave a DOUBLE space behind. Splitting
# on a literal " " would then emit an empty token that no chatter list removes, and every
# such listing would land in a different entity than its plain-template siblings (measured:
# exactly 2 entities per canonical). Collapse whitespace first, and drop "" defensively.
title_tokens = array_distinct(
    split(
        regexp_replace(
            trim(col("title_clean")),
            r"\s+",
            " "
        ),
        " "
    )
)

city_tokens = array_distinct(
    split(
        regexp_replace(
            lower(
                trim(
                    coalesce(col("location_city"), lit(""))
                )
            ),
            r"\s+",
            " "
        ),
        " "
    )
)

product_tokens = array_except(
    array_except(
        array_except(
            title_tokens,
            city_tokens
        ),
        lit(CHATTER)
    ),
    lit([""])
)

# If a title is nothing but chatter, fall back to its raw token set rather than
# collapsing every such listing into one empty-key entity.
model_key_tokens = when(
    size(product_tokens) == 0,
    sort_array(title_tokens)
).otherwise(
    sort_array(product_tokens)
)

df = df.withColumn(
    "model_key",
    concat_ws(" ", model_key_tokens)
)


# ============================================================
# MODEL -> ENTITY
# ============================================================

print("\n========== UNIQUE MODELS ==========")

model_mapping = (
    df
    .select("model_key")
    .distinct()
    .withColumn(
        "entity_id",
        row_number().over(
            Window.orderBy("model_key")
        )
    )
)

print(
    "Unique model keys:",
    model_mapping.count()
)


# ============================================================
# MAP ENTITIES TO LISTINGS
# ============================================================
# Direct join on the per-row key (no title indirection = no duplicate rows).

df = df.join(
    model_mapping,
    on="model_key",
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
