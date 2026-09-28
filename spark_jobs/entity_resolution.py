import os
import re
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. WINDOWS HADOOP SETUP
# ============================================================

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

HADOOP_HOME = os.path.join(PROJECT_ROOT, "hadoop")

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME

hadoop_bin = os.path.join(HADOOP_HOME, "bin")

if hadoop_bin not in os.environ["PATH"]:
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]


# ============================================================
# 2. PATHS
# ============================================================

INPUT_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "clean_listings.parquet"
)

OUTPUT_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "entity_resolved.parquet"
)


# ============================================================
# 3. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-EntityResolution")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.debug.maxToStringFields", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 4. READ CLEAN DATA
# ============================================================

print("=" * 60)
print("STAGE 2: ENTITY RESOLUTION")
print("=" * 60)

print("\nReading:")
print(INPUT_PATH)

df = spark.read.parquet(INPUT_PATH)

print("\nInput schema:")
df.printSchema()

print("\nInput columns:")
print(df.columns)


# ============================================================
# 5. CREATE MODEL KEY
# ============================================================

print("\nCreating normalized model keys...")


def normalize_model_key(column):
    """
    Normalize product title while preserving important
    distinctions such as Pro vs Pro Max.
    """

    result = F.lower(F.col(column))

    # Remove common storage-size expressions
    result = F.regexp_replace(
        result,
        r"\b\d+\s*(gb|tb)\b",
        " "
    )

    # Remove common color names
    result = F.regexp_replace(
        result,
        r"\b(black|white|red|blue|green|yellow|purple|pink|gold|silver|gray|grey|orange|brown)\b",
        " "
    )

    # Remove common condition phrases
    result = F.regexp_replace(
        result,
        r"\b(new|used|like new|excellent condition|good condition|fair condition|mint condition)\b",
        " "
    )

    # Normalize punctuation
    result = F.regexp_replace(
        result,
        r"[^a-z0-9 ]",
        " "
    )

    # Normalize whitespace
    result = F.trim(
        F.regexp_replace(result, r"\s+", " ")
    )

    return result


df = df.withColumn(
    "model_key",
    normalize_model_key("title_clean")
)


# ============================================================
# 6. PROTECT PRO / PRO MAX DISTINCTION
# ============================================================

print("Protecting Pro / Pro Max distinctions...")

df = df.withColumn(
    "model_key",
    F.regexp_replace(
        "model_key",
        r"\bpro max\b",
        "pro_max"
    )
)

df = df.withColumn(
    "model_key",
    F.regexp_replace(
        "model_key",
        r"\bpro\b",
        "pro"
    )
)


# ============================================================
# 7. REMOVE EMPTY MODEL KEYS
# ============================================================

df = df.withColumn(
    "model_key",
    F.when(
        F.length(F.trim(F.col("model_key"))) > 0,
        F.col("model_key")
    ).otherwise(F.lit(None))
)


# ============================================================
# 8. MINHASH LSH
# ============================================================

print("\nRunning MinHash LSH entity matching...")

from pyspark.ml.feature import HashingTF, MinHashLSH


# Get unique model keys only
model_keys = (
    df
    .select("model_key")
    .where(F.col("model_key").isNotNull())
    .dropDuplicates()
)


# Convert model keys into token sets
tokenized = model_keys.withColumn(
    "tokens",
    F.split(F.col("model_key"), " ")
)


hashing_tf = HashingTF(
    inputCol="tokens",
    outputCol="features",
    numFeatures=1 << 18
)

hashed = hashing_tf.transform(tokenized)


minhash = MinHashLSH(
    inputCol="features",
    outputCol="hashes",
    numHashTables=3
)

minhash_model = minhash.fit(hashed)


# Approximate similarity pairs
similar_pairs = (
    minhash_model.approxSimilarityJoin(
        hashed,
        hashed,
        0.4,
        distCol="JaccardDistance"
    )
    .select(
        F.col("datasetA.model_key").alias("model_key_a"),
        F.col("datasetB.model_key").alias("model_key_b"),
        F.col("JaccardDistance")
    )
    .where(
        F.col("model_key_a") != F.col("model_key_b")
    )
)


print("MinHash similarity pairs generated.")


# ============================================================
# 9. CREATE ENTITY IDs
# ============================================================

print("\nCreating entity IDs...")

# Each normalized model key gets a stable entity ID.
# MinHash pairs are used for similarity analysis, while
# exact normalized keys remain the canonical grouping key.

entity_keys = (
    model_keys
    .withColumn(
        "entity_id",
        F.sha2(F.col("model_key"), 256)
    )
)


df = (
    df
    .join(
        entity_keys,
        on="model_key",
        how="left"
    )
)


# ============================================================
# 10. REPOST TITLE KEY
# ============================================================

print("Creating repost title keys...")

df = df.withColumn(
    "repost_title_key",
    F.lower(F.col("title_clean"))
)

# Remove common repost wording
df = df.withColumn(
    "repost_title_key",
    F.regexp_replace(
        "repost_title_key",
        r"\bmust go\b",
        " "
    )
)

df = df.withColumn(
    "repost_title_key",
    F.regexp_replace(
        "repost_title_key",
        r"\brelist\b",
        " "
    )
)

# Remove trailing exclamation marks
df = df.withColumn(
    "repost_title_key",
    F.regexp_replace(
        "repost_title_key",
        r"!+$",
        ""
    )
)

df = df.withColumn(
    "repost_title_key",
    F.trim(
        F.regexp_replace(
            "repost_title_key",
            r"\s+",
            " "
        )
    )
)


# ============================================================
# 11. SELLER + TITLE REPOST DETECTION
# ============================================================

print("Detecting seller/title reposts...")


window_spec = (
    Window
    .partitionBy(
        "seller_id",
        "repost_title_key"
    )
    .orderBy(
        F.col("posted_date").asc()
    )
)


df = df.withColumn(
    "previous_posted_date",
    F.lag("posted_date").over(window_spec)
)


# Determine whether listing is a repost.
#
# A listing is treated as a repost when:
# - seller_id exists
# - current posted_date exists
# - previous posted_date exists
# - previous listing is within 1-14 days

df = df.withColumn(
    "is_repost",
    F.when(
        (
            F.col("seller_id").isNotNull()
            & F.col("posted_date").isNotNull()
            & F.col("previous_posted_date").isNotNull()
            & (
                F.datediff(
                    F.col("posted_date"),
                    F.col("previous_posted_date")
                ).between(1, 14)
            )
        ),
        F.lit(True)
    ).otherwise(F.lit(False))
)


# ============================================================
# 12. ORIGINAL LISTING ID
# ============================================================

df = df.withColumn(
    "original_listing_id",
    F.first(
        "listing_id",
        ignorenulls=True
    ).over(
        window_spec.rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing
        )
    )
)


# For non-reposts, the listing itself is the original.
df = df.withColumn(
    "original_listing_id",
    F.when(
        F.col("is_repost") == True,
        F.col("original_listing_id")
    ).otherwise(
        F.col("listing_id")
    )
)


# ============================================================
# 13. REMOVE TEMPORARY COLUMNS
# ============================================================

df = df.drop(
    "previous_posted_date",
    "repost_title_key"
)


# ============================================================
# 14. SELECT FINAL OUTPUT COLUMNS
# ============================================================

print("\nPreparing final entity-resolved dataset...")

final_columns = [
    "listing_id",
    "title",
    "description",
    "price",
    "price_raw",
    "currency",
    "category",
    "sub_category",
    "category_full",
    "item_condition_id",
    "brand_name",
    "shipping",
    "posted_date",
    "delisted_date",
    "location_city",
    "location_region",
    "seller_type",
    "seller_id",
    "source_platform",
    "title_clean",
    "description_clean",
    "model_key",
    "entity_id",
    "is_repost",
    "original_listing_id"
]

# Keep only columns that actually exist
final_columns = [
    c for c in final_columns
    if c in df.columns
]

df_final = df.select(*final_columns)


# ============================================================
# 15. OUTPUT SUMMARY
# ============================================================

print("\n========== ENTITY RESOLUTION SUMMARY ==========")

print("Input records:")
print(df.count())

print("\nOutput columns:")
print(df_final.columns)

print("\nRepost statistics:")

repost_stats = (
    df_final
    .groupBy("is_repost")
    .count()
    .orderBy("is_repost")
)

repost_stats.show()


print("\nEntity count:")

entity_count = (
    df_final
    .select("entity_id")
    .where(F.col("entity_id").isNotNull())
    .distinct()
    .count()
)

print(entity_count)


# ============================================================
# 16. SAMPLE
# ============================================================

print("\nSample entity-resolved records:")

df_final.select(
    "listing_id",
    "title",
    "model_key",
    "entity_id",
    "is_repost",
    "original_listing_id"
).show(10, truncate=False)


# ============================================================
# 17. REMOVE OLD OUTPUT
# ============================================================

if os.path.exists(OUTPUT_PATH):
    print("\nRemoving existing output...")
    shutil.rmtree(OUTPUT_PATH)


# ============================================================
# 18. WRITE OUTPUT
# ============================================================

print("\nWriting entity-resolved Parquet output:")

print(OUTPUT_PATH)

(
    df_final
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


# ============================================================
# 19. FINAL VERIFICATION
# ============================================================

print("\nVerifying output...")

output_df = spark.read.parquet(OUTPUT_PATH)

output_count = output_df.count()

print("Output records:", output_count)

print("\nOutput location:")
print(OUTPUT_PATH)


print("\n==============================================")
print("STAGE 2 ENTITY RESOLUTION COMPLETE")
print("==============================================")

spark.stop()