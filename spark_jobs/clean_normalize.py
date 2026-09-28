from pathlib import Path
import os
import shutil
import glob

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType
)


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

MERCARI_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "mercari"
    / "train.tsv"
)

GENERATED_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "generated"
    / "run=2026_09_28T150245Z"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "clean_listings.parquet"
)


# ============================================================
# WINDOWS / HADOOP
# ============================================================

HADOOP_HOME = PROJECT_ROOT / "hadoop"

os.environ["HADOOP_HOME"] = str(HADOOP_HOME)

os.environ["PATH"] = (
    str(HADOOP_HOME / "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-Clean-Normalize")

    .master("local[*]")

    .config(
        "spark.driver.memory",
        "4g"
    )

    .config(
        "spark.executor.memory",
        "4g"
    )

    .config(
        "spark.hadoop.io.native.lib.available",
        "false"
    )

    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )

    .config(
        "spark.sql.adaptive.enabled",
        "true"
    )

    .config(
        "spark.sql.shuffle.partitions",
        "16"
    )

    .config(
        "spark.sql.debug.maxToStringFields",
        "200"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print()
print("==============================================")
print("RESELLRADAR - STAGE 1")
print("CLEAN + NORMALIZE")
print("==============================================")


# ============================================================
# EXACT 19-FIELD SCHEMA
# ============================================================

schema = StructType([

    StructField(
        "listing_id",
        StringType(),
        True
    ),

    StructField(
        "title",
        StringType(),
        True
    ),

    StructField(
        "description",
        StringType(),
        True
    ),

    StructField(
        "price",
        DoubleType(),
        True
    ),

    StructField(
        "price_raw",
        StringType(),
        True
    ),

    StructField(
        "currency",
        StringType(),
        True
    ),

    StructField(
        "category",
        StringType(),
        True
    ),

    StructField(
        "sub_category",
        StringType(),
        True
    ),

    StructField(
        "category_full",
        StringType(),
        True
    ),

    StructField(
        "item_condition_id",
        IntegerType(),
        True
    ),

    StructField(
        "brand_name",
        StringType(),
        True
    ),

    StructField(
        "shipping",
        IntegerType(),
        True
    ),

    StructField(
        "posted_date",
        TimestampType(),
        True
    ),

    StructField(
        "delisted_date",
        TimestampType(),
        True
    ),

    StructField(
        "location_city",
        StringType(),
        True
    ),

    StructField(
        "location_region",
        StringType(),
        True
    ),

    StructField(
        "seller_type",
        StringType(),
        True
    ),

    StructField(
        "seller_id",
        StringType(),
        True
    ),

    StructField(
        "source_platform",
        StringType(),
        False
    )
])


# ============================================================
# READ MERCARI
# ============================================================

print()
print("========== READING MERCARI ==========")

print(
    f"File: {MERCARI_FILE}"
)


mercari_raw = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .option("inferSchema", True)
    .csv(str(MERCARI_FILE))
)


mercari_count = mercari_raw.count()


print(
    f"Mercari raw records: "
    f"{mercari_count:,}"
)


# ============================================================
# MERCARI MAPPING
# ============================================================

mercari = (
    mercari_raw
    .select(

        F.col("train_id")
        .cast("string")
        .alias("listing_id"),

        F.col("name")
        .cast("string")
        .alias("title"),

        F.when(
            F.col("item_description") == "[rm]",
            F.lit(None)
        )
        .otherwise(
            F.col("item_description")
            .cast("string")
        )
        .alias("description"),

        F.col("price")
        .cast("double")
        .alias("price"),

        F.lit(None)
        .cast("string")
        .alias("price_raw"),

        F.lit("USD")
        .alias("currency"),

        F.split(
            F.col("category_name"),
            "/"
        ).getItem(0)
        .alias("category"),

        F.split(
            F.col("category_name"),
            "/"
        ).getItem(2)
        .alias("sub_category"),

        F.col("category_name")
        .cast("string")
        .alias("category_full"),

        F.col("item_condition_id")
        .cast("int")
        .alias("item_condition_id"),

        F.col("brand_name")
        .cast("string")
        .alias("brand_name"),

        F.col("shipping")
        .cast("int")
        .alias("shipping"),

        F.lit(None)
        .cast("timestamp")
        .alias("posted_date"),

        F.lit(None)
        .cast("timestamp")
        .alias("delisted_date"),

        F.lit(None)
        .cast("string")
        .alias("location_city"),

        F.lit(None)
        .cast("string")
        .alias("location_region"),

        F.lit(None)
        .cast("string")
        .alias("seller_type"),

        F.lit(None)
        .cast("string")
        .alias("seller_id"),

        F.lit("mercari")
        .alias("source_platform")
    )
)


# ============================================================
# READ GENERATED JSONL
# ============================================================

print()
print("========== READING GENERATED DATA ==========")

print(
    f"Directory: {GENERATED_DIR}"
)


generated_files = sorted(
    glob.glob(
        str(
            GENERATED_DIR
            / "*.jsonl"
        )
    )
)


print(
    f"Generated JSONL files found: "
    f"{len(generated_files)}"
)


if len(generated_files) == 0:

    raise FileNotFoundError(
        "No generated JSONL files found."
    )


generated_raw = (
    spark.read
    .schema(schema)
    .json(generated_files)
)


generated_count = generated_raw.count()


print(
    f"Generated raw records: "
    f"{generated_count:,}"
)


# ============================================================
# UNION
# ============================================================

print()
print("========== UNIONING DATA ==========")


df = (
    mercari
    .unionByName(
        generated_raw,
        allowMissingColumns=True
    )
)


raw_count = (
    mercari_count
    + generated_count
)


print(
    f"Total raw records: "
    f"{raw_count:,}"
)


# ============================================================
# SOURCE COUNTS
# ============================================================

print()
print("========== SOURCE COUNTS ==========")


source_counts = (
    df
    .groupBy("source_platform")
    .count()
    .orderBy("source_platform")
)


for row in source_counts.collect():

    print(
        f"{row['source_platform']:<10} "
        f"{row['count']:,}"
    )


# ============================================================
# TEXT CLEANING
# ============================================================

print()
print("========== TEXT CLEANING ==========")


df = (
    df

    .withColumn(
        "title_clean",

        F.regexp_replace(

            F.lower(
                F.trim(
                    F.col("title")
                )
            ),

            r"[^A-Za-z0-9 ]",

            ""
        )
    )

    .withColumn(
        "description_clean",

        F.regexp_replace(

            F.lower(
                F.trim(
                    F.col("description")
                )
            ),

            r"[^A-Za-z0-9 ]",

            ""
        )
    )
)


# ============================================================
# FILTER
# ============================================================

print()
print("========== FILTERING ==========")


df = (
    df

    .filter(
        F.col("listing_id").isNotNull()
    )

    .filter(
        F.col("title").isNotNull()
    )

    .filter(
        F.col("price").isNotNull()
    )
)


after_filter = df.count()


print(
    f"After filtering: "
    f"{after_filter:,}"
)


print(
    f"Removed: "
    f"{raw_count - after_filter:,}"
)


# ============================================================
# DEDUPLICATION
# ============================================================

print()
print("========== REMOVING DUPLICATES ==========")


df = df.dropDuplicates(
    ["listing_id"]
)


clean_count = df.count()


print(
    f"After deduplication: "
    f"{clean_count:,}"
)


print(
    f"Duplicates removed: "
    f"{after_filter - clean_count:,}"
)


# ============================================================
# CLEAN SOURCE COUNTS
# ============================================================

print()
print("========== CLEAN SOURCE COUNTS ==========")


clean_source_counts = (
    df
    .groupBy("source_platform")
    .count()
    .orderBy("source_platform")
)


for row in clean_source_counts.collect():

    print(
        f"{row['source_platform']:<10} "
        f"{row['count']:,}"
    )


# ============================================================
# SAMPLE
# ============================================================

print()
print("========== SAMPLE RECORDS ==========")


df.select(
    "listing_id",
    "title",
    "price",
    "category",
    "sub_category",
    "brand_name",
    "source_platform"
).show(
    10,
    truncate=True
)


# ============================================================
# OUTPUT DIRECTORY
# ============================================================

print()
print("========== PREPARING OUTPUT ==========")


if OUTPUT_DIR.exists():

    shutil.rmtree(
        OUTPUT_DIR
    )


OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# REPARTITION
# ============================================================

print()
print("Repartitioning dataframe...")


df = df.repartition(8)


print(
    "Repartition complete."
)


# ============================================================
# PYARROW
# ============================================================

print()
print("==============================================")
print("SAVING PARQUET USING PYARROW")
print("==============================================")


import pyarrow as pa
import pyarrow.parquet as pq


BATCH_SIZE = 5000


row_iterator = (
    df.toLocalIterator()
)


batch = []

part_number = 0

total_written = 0


# ============================================================
# WRITE BATCH
# ============================================================

def write_batch(
    rows,
    part_number
):

    if not rows:
        return 0

    table = (
        pa.Table.from_pylist(
            rows
        )
    )

    output_file = (
        OUTPUT_DIR
        / f"part-{part_number:05d}.parquet"
    )

    pq.write_table(
        table,
        str(output_file),
        compression="snappy"
    )

    return len(rows)


# ============================================================
# STREAM + WRITE
# ============================================================

for row in row_iterator:

    batch.append(
        row.asDict(
            recursive=True
        )
    )

    if len(batch) >= BATCH_SIZE:

        written = write_batch(
            batch,
            part_number
        )

        total_written += written

        print(
            f"Written "
            f"part-{part_number:05d}.parquet "
            f"| {written:,} records "
            f"| Total: {total_written:,}"
        )

        part_number += 1

        batch = []


# ============================================================
# LAST BATCH
# ============================================================

if batch:

    written = write_batch(
        batch,
        part_number
    )

    total_written += written

    print(
        f"Written "
        f"part-{part_number:05d}.parquet "
        f"| {written:,} records "
        f"| Total: {total_written:,}"
    )

    part_number += 1


# ============================================================
# VERIFY
# ============================================================

print()
print("==============================================")
print("VERIFYING OUTPUT")
print("==============================================")


parquet_files = sorted(
    OUTPUT_DIR.glob(
        "part-*.parquet"
    )
)


print()
print(
    f"Parquet files created: "
    f"{len(parquet_files)}"
)


print(
    f"Total records written: "
    f"{total_written:,}"
)


total_output_size = sum(
    file.stat().st_size
    for file in parquet_files
)


print(
    f"Total output size: "
    f"{total_output_size / (1024 * 1024):.2f} MB"
)


# ============================================================
# FINAL CHECK
# ============================================================

print()
print("========== FINAL CHECK ==========")


if total_written == clean_count:

    print(
        "SUCCESS: Record counts match."
    )

else:

    print(
        "WARNING: Record counts do not match."
    )

    print(
        f"Clean records: "
        f"{clean_count:,}"
    )

    print(
        f"Written records: "
        f"{total_written:,}"
    )


# ============================================================
# COMPLETE
# ============================================================

print()
print("==============================================")
print("STAGE 1 COMPLETE")
print("==============================================")


print()
print(
    f"Raw records: "
    f"{raw_count:,}"
)

print(
    f"Clean records: "
    f"{clean_count:,}"
)

print(
    f"Written records: "
    f"{total_written:,}"
)

print()
print(
    "Output directory:"
)

print(
    OUTPUT_DIR
)

print()
print(
    "=============================================="
)


# ============================================================
# STOP
# ============================================================

spark.stop()