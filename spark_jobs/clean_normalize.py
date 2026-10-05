import os

os.environ["HADOOP_HOME"] = os.getcwd() + "\\hadoop"
os.environ["hadoop.home.dir"] = os.getcwd() + "\\hadoop"

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, TimestampType
)
from pyspark.sql.functions import (
    col, lower, trim,
    regexp_replace, to_timestamp,
    lit, split, when
)

BASE_DIR = Path(__file__).resolve().parent.parent

MERCARI_PATH = "hdfs://namenode:9000/data/raw/mercari/train.tsv"
GENERATED_PATH = (
    "hdfs://namenode:9000/data/raw/generated/"
    "run=2026_09_28T150245Z/*.jsonl"
)

# Cluster batch mode: RR_DATA_ROOT (e.g. hdfs://namenode:9000/data) redirects the
# Stage-1 output into the HDFS processed zone instead of the local checkout.
_DATA_ROOT = os.environ.get("RR_DATA_ROOT")

OUTPUT_PATH = (
    f"{_DATA_ROOT}/processed/clean_listings.parquet"
    if _DATA_ROOT
    else str(
        BASE_DIR / "data" / "processed" / "clean_listings.parquet"
    )
)

print("Reading Mercari:")
print(MERCARI_PATH)

print("\nReading Generated:")
print(GENERATED_PATH)

spark = (
    SparkSession.builder
    .appName("ResellRadar-CleanNormalize")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config(
        "spark.hadoop.io.native.lib.available",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# UNION SCHEMA — 19 FIELDS
# ============================================================

raw_schema = StructType([
    StructField("listing_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("price_raw", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("category_full", StringType(), True),
    StructField("item_condition_id", IntegerType(), True),
    StructField("brand_name", StringType(), True),
    StructField("shipping", IntegerType(), True),
    StructField("posted_date", TimestampType(), True),
    StructField("delisted_date", TimestampType(), True),
    StructField("location_city", StringType(), True),
    StructField("location_region", StringType(), True),
    StructField("seller_type", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("source_platform", StringType(), False)
])


# ============================================================
# MERCARI
# ============================================================

mercari_raw = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv(MERCARI_PATH)
)

mercari_df = (
    mercari_raw
    .select(
        col("train_id")
            .cast("string")
            .alias("listing_id"),

        col("name")
            .alias("title"),

        when(
            col("item_description") == "[rm]",
            None
        ).otherwise(
            col("item_description")
        ).alias("description"),

        col("price")
            .cast("double")
            .alias("price"),

        lit(None)
            .cast("string")
            .alias("price_raw"),

        lit("USD")
            .alias("currency"),

        split(
            col("category_name"),
            "/"
        ).getItem(0)
        .alias("category"),

        split(
            col("category_name"),
            "/"
        ).getItem(2)
        .alias("sub_category"),

        col("category_name")
            .alias("category_full"),

        col("item_condition_id")
            .cast("int")
            .alias("item_condition_id"),

        col("brand_name")
            .alias("brand_name"),

        col("shipping")
            .cast("int")
            .alias("shipping"),

        lit(None)
            .cast("timestamp")
            .alias("posted_date"),

        lit(None)
            .cast("timestamp")
            .alias("delisted_date"),

        lit(None)
            .cast("string")
            .alias("location_city"),

        lit(None)
            .cast("string")
            .alias("location_region"),

        lit(None)
            .cast("string")
            .alias("seller_type"),

        lit(None)
            .cast("string")
            .alias("seller_id"),

        lit("mercari")
            .alias("source_platform")
    )
)


# ============================================================
# GENERATED
# ============================================================

generated_df = (
    spark.read
    .json(GENERATED_PATH)
    .select(
        col("listing_id").cast("string"),
        col("title"),
        col("description"),
        col("price").cast("double"),
        col("price_raw"),
        col("currency"),
        col("category"),
        col("sub_category"),
        col("category_full"),
        col("item_condition_id").cast("int"),
        col("brand_name"),
        col("shipping").cast("int"),
        to_timestamp("posted_date").alias("posted_date"),
        to_timestamp("delisted_date").alias("delisted_date"),
        col("location_city"),
        col("location_region"),
        col("seller_type"),
        col("seller_id"),
        col("source_platform")
    )
)


# ============================================================
# UNION
# ============================================================

df = mercari_df.unionByName(generated_df)

print("\n========== RAW DATA ==========")
print("Total Records:", df.count())


# ============================================================
# REQUIRED FIELDS ONLY
# ============================================================

df = df.dropna(
    subset=[
        "listing_id",
        "title",
        "price"
    ]
)


# ============================================================
# TEXT CLEANING
# ============================================================

df = df.withColumn(
    "title_clean",
    lower(
        trim(
            regexp_replace(
                col("title"),
                "[^A-Za-z0-9 ]",
                ""
            )
        )
    )
)

df = df.withColumn(
    "description_clean",
    lower(
        trim(
            regexp_replace(
                col("description"),
                "[^A-Za-z0-9 ]",
                ""
            )
        )
    )
)


# ============================================================
# REMOVE DUPLICATE LISTINGS
# ============================================================

df = df.dropDuplicates(
    ["listing_id"]
)

print(
    "Clean Records:",
    df.count()
)


# ============================================================
# SAMPLE
# ============================================================

print("\n========== SAMPLE ==========")

df.select(
    "listing_id",
    "source_platform",
    "title",
    "title_clean",
    "price"
).show(
    10,
    truncate=False
)


# ============================================================
# SAVE
# ============================================================

df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_PATH
)

print("\n========== SUCCESS ==========")

print("Saved to:")
print(OUTPUT_PATH)

spark.stop()