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
    lit
)

# -------------------------------------------------
# Project paths
# -------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

# HDFS input paths (Person 1 handoff)
MERCARI_PATH = "hdfs://namenode:9000/data/raw/mercari/train.tsv"
GENERATED_PATH = "hdfs://namenode:9000/data/raw/generated/run=2026_09_28T150245Z/*.jsonl"

# Local output
OUTPUT_PATH = str(BASE_DIR / "data" / "processed" / "clean_listings.parquet")

print("Reading Mercari:")
print(MERCARI_PATH)

print("\nReading Generated:")
print(GENERATED_PATH)

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("ResellRadar-CleanNormalize")
    .master("local[*]")
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------------
# 19-column Union Schema
# -------------------------------------------------
raw_schema = StructType([
    StructField("listing_id", StringType(), True),
    StructField("source_platform", StringType(), False),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("price_raw", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("category_full", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("item_condition_id", IntegerType(), True),
    StructField("shipping", IntegerType(), True),
    StructField("seller_id", StringType(), True),
    StructField("location_city", StringType(), True),
    StructField("location_region", StringType(), True),
    StructField("posted_date", TimestampType(), True),
    StructField("delisted_date", TimestampType(), True),
    StructField("image_url", StringType(), True),
    StructField("product_url", StringType(), True)
])

# -------------------------------------------------
# Read Mercari TSV
# -------------------------------------------------
mercari_df = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .csv(MERCARI_PATH)
    .select(
        col("train_id").cast("string").alias("listing_id"),
        lit("mercari").alias("source_platform"),
        col("name").alias("title"),
        col("item_description").alias("description"),
        col("price").cast("double").alias("price"),
        lit(None).cast("string").alias("price_raw"),
        lit("USD").alias("currency"),
        col("brand_name").alias("brand_name"),
        col("category_name").alias("category_full"),
        lit(None).cast("string").alias("sub_category"),
        col("item_condition_id").cast("int").alias("item_condition_id"),
        col("shipping").cast("int").alias("shipping"),
        lit(None).cast("string").alias("seller_id"),
        lit(None).cast("string").alias("location_city"),
        lit(None).cast("string").alias("location_region"),
        lit(None).cast("timestamp").alias("posted_date"),
        lit(None).cast("timestamp").alias("delisted_date"),
        lit(None).cast("string").alias("image_url"),
        lit(None).cast("string").alias("product_url")
    )
)

# -------------------------------------------------
# Read Generated JSONL
# -------------------------------------------------
generated_df = (
    spark.read
    .json(GENERATED_PATH)
    .select(
        col("listing_id").cast("string"),
        col("source_platform"),
        col("title"),
        col("description"),
        col("price").cast("double"),
        col("price_raw"),
        col("currency"),
        col("brand_name"),
        col("category_full"),
        col("sub_category"),
        col("item_condition_id").cast("int"),
        col("shipping").cast("int"),
        col("seller_id"),
        col("location_city"),
        col("location_region"),
        to_timestamp("posted_date").alias("posted_date"),
        to_timestamp("delisted_date").alias("delisted_date"),
        col("image_url"),
        col("product_url")
    )
)

# -------------------------------------------------
# Union Both Sources
# -------------------------------------------------
df = mercari_df.unionByName(generated_df)

print("\n========== RAW DATA ==========")
print("Total Records:", df.count())

# -------------------------------------------------
# Drop only essential nulls
# -------------------------------------------------
df = df.dropna(subset=[
    "listing_id",
    "title",
    "price"
])

# -------------------------------------------------
# Clean Title
# -------------------------------------------------
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

# -------------------------------------------------
# Clean Description
# -------------------------------------------------
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

# -------------------------------------------------
# Remove duplicate listings
# -------------------------------------------------
df = df.dropDuplicates(["listing_id"])

print("Clean Records:", df.count())

# -------------------------------------------------
# Preview
# -------------------------------------------------
print("\n========== SAMPLE ==========")

df.select(
    "listing_id",
    "source_platform",
    "title",
    "title_clean",
    "price"
).show(10, truncate=False)

# -------------------------------------------------
# Save Parquet
# -------------------------------------------------
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("\n========== SUCCESS ==========")
print("Saved to:")
print(OUTPUT_PATH)

spark.stop()