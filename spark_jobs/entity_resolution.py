"""
ResellRadar - Stage 2 entity resolution (canonical job).

Reads :  data/processed/clean_listings.parquet
         data/processed/chatter_vocab.json      (built by scripts/learn_chatter_vocab.py)
Writes:  data/processed/entity_resolved.parquet (same schema/path as the original upload, so
                                                  Stage 3 and the dashboard need no change)

This file replaces the original upload after the 2026-09-29 acceptance run found two
defects in it (full evidence: logs/acceptance_2026_09_29.md, docs/HANDOFF_Person2.md 4c):

F1 - the original's MinHashLSH approxSimilarityJoin was display-only dead code (entity_id
     came from row_number over model_key, provably identical counts) while costing
     17,360,494,325 candidate pairs on the delivered data - the job never finished.
F2 - the original's model_key regexes stripped storage sizes and colours (the product
     identity per docs/SCHEMA_generated.md) and kept the listing chatter, splitting every
     true product across ~39 keys (Task A F1 0.2341).

What this version does
----------------------
1. ENTITY KEY = PRODUCT TOKENS, NOT A DESTRUCTIVELY STRIPPED STRING.
   Tokenize title_clean, remove the listing's own location_city, remove the chatter
   vocabulary, then key on the sorted token SET. Storage, colour and model are kept because
   they ARE the product (canonical = brand x model x storage x colour; titles are surface
   variants of it). Set semantics also collapse the generator's "last-token-first" variant.
   Order-insensitive and de-duplicated via array_except.

2. CHATTER VOCABULARY IS LEARNED FROM THE CORPUS, NOT HARDCODED (v3 improvement).
   data/processed/chatter_vocab.json is produced by scripts/learn_chatter_vocab.py from the
   Stage 1 output alone (never from ground truth): a token/phrase is chatter iff deleting it
   from a title yields the exact sorted token set of another real title from the same
   source, in >= 85% of the titles that contain it (>= 50 distinct-title support, 1-3-token
   phrases). Product-line suffixes (pro/max/plus/mini/ultra/...) are guarded at learn time:
   surface-optional in text but catalog identity. Per-source vocabularies (generated 53
   tokens incl. all 27 documented ones + city names; mercari 492 real-marketplace chatter
   tokens). The job FAILS LOUDLY if the artifact is missing - build it first with:
       python scripts/learn_chatter_vocab.py

3. NO TITLE INDIRECTION AND NO DEAD SELF-JOIN.
   The original mapped title -> entity and joined listings on title_clean (silently
   duplicating rows if one title ever mapped to two entities) around a 17-billion-pair
   demo join. The key here is a per-row function, so listings join the entity table
   directly on model_key.

4. WHITESPACE IS COLLAPSED BEFORE TOKENIZING.
   clean_normalize strips punctuation but keeps the spaces around it, so the " - Unlocked" /
   " - {city} pickup" / " - {condition}" templates leave a DOUBLE space. Spark's
   split(col, " ") then emits an empty token that no vocabulary removes and every product
   split into exactly 2 entities (measured). Collapse whitespace first, drop "" defensively.

Repost detection is byte-identical to the original upload (same window, same 1-14 day rule,
same original-listing link), so repost scoring is unaffected by this rewrite.

Known accepted limitation (assessed, not a defect at this scale): the entity-id
row_number() over Window.orderBy("model_key") has no partitionBy, so unique keys flow
through a single partition. It is deterministic and costs seconds for ~1M keys; a
distributed ID scheme would trade determinism for a non-bottleneck. Revisit only at
~100x corpus scale.

Acceptance evidence: logs/acceptance_2026_09_29.md (v2 fixed-list parity 0.9802,
v3 learned-vocabulary 0.9802), logs/er_report_stage2_v3.md.
"""

import json
import os
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
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

# Cluster batch mode: RR_DATA_ROOT (e.g. hdfs://namenode:9000/data) points input and
# output at the HDFS zones so the executors on the worker containers read/write the
# cluster storage. Default stays the local checkout (single-unit execution).
_DATA_ROOT = os.environ.get("RR_DATA_ROOT")

INPUT_PATH = (
    f"{_DATA_ROOT}/processed/clean_listings.parquet"
    if _DATA_ROOT
    else str(
        BASE_DIR / "data" / "processed" / "clean_listings.parquet"
    )
)

OUTPUT_PATH = (
    f"{_DATA_ROOT}/processed/entity_resolved.parquet"
    if _DATA_ROOT
    else str(
        BASE_DIR / "data" / "processed" / "entity_resolved.parquet"
    )
)

VOCAB_PATH = BASE_DIR / "data" / "processed" / "chatter_vocab.json"


# ============================================================
# CHATTER VOCABULARY (learned from the corpus - build artifact)
# ============================================================
# Produced by scripts/learn_chatter_vocab.py from the Stage 1 output alone.
# Fail loudly rather than silently keying with an empty vocabulary.

if not VOCAB_PATH.exists():
    raise SystemExit(
        f"FATAL: chatter vocabulary not found: {VOCAB_PATH}\n"
        f"  entity_resolution keys listings with a corpus-derived vocabulary.\n"
        f"  Build it first:  python scripts/learn_chatter_vocab.py"
    )

_vocab_payload = json.loads(VOCAB_PATH.read_text(encoding="utf-8"))
VOCABULARIES = _vocab_payload.get("vocabularies")
if not isinstance(VOCABULARIES, dict) or not VOCABULARIES:
    raise SystemExit(
        f"FATAL: {VOCAB_PATH} has no usable 'vocabularies' mapping. "
        f"Rebuild it:  python scripts/learn_chatter_vocab.py"
    )

print("Chatter vocabulary (learned, per source):")
for _src in sorted(VOCABULARIES):
    print(f"  {_src}: {len(VOCABULARIES[_src])} tokens")
print(f"  source: {VOCAB_PATH.name} (created {_vocab_payload.get('created_utc', '?')})")


# ============================================================
# START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-EntityResolution")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
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
# title tokens, minus the listing's own city, minus the row's SOURCE-SPECIFIC
# learned chatter vocabulary. array_except is a set operation, so tokens are
# de-duplicated and order-insensitive:
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

# Per-source vocabulary: generated and mercari titles carry different chatter.
# Rows from an unseen source get no chatter removed (safe default; the
# all-chatter fallback below still prevents an empty key).
chatter_for_source = lit([]).cast(ArrayType(StringType()))
for _src in sorted(VOCABULARIES, reverse=True):
    chatter_for_source = when(
        col("source_platform") == _src,
        lit(sorted(VOCABULARIES[_src]))
    ).otherwise(chatter_for_source)

product_tokens = array_except(
    array_except(
        array_except(
            title_tokens,
            city_tokens
        ),
        chatter_for_source
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
