import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. PROJECT PATH
# ============================================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)


# ============================================================
# 2. WINDOWS HADOOP SETUP
# ============================================================

HADOOP_HOME = os.path.join(PROJECT_ROOT, "hadoop")

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME

HADOOP_BIN = os.path.join(HADOOP_HOME, "bin")

if HADOOP_BIN not in os.environ["PATH"]:
    os.environ["PATH"] = (
        HADOOP_BIN
        + os.pathsep
        + os.environ["PATH"]
    )


# ============================================================
# 3. PATHS
# ============================================================

INPUT_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "entity_resolved.parquet"
)

CURATED_DIR = os.path.join(
    PROJECT_ROOT,
    "data",
    "curated"
)

DEPRECIATION_PATH = os.path.join(
    CURATED_DIR,
    "depreciation_curve_curated.parquet"
)

VELOCITY_PATH = os.path.join(
    CURATED_DIR,
    "resale_velocity_curated.parquet"
)

REGIONAL_PATH = os.path.join(
    CURATED_DIR,
    "regional_price_variance_curated.parquet"
)


# ============================================================
# 4. CREATE CURATED DIRECTORY
# ============================================================

os.makedirs(CURATED_DIR, exist_ok=True)


# ============================================================
# 5. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("ResellRadar-FeatureEngineering")
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
# 6. READ ENTITY-RESOLVED DATA
# ============================================================

print("=" * 60)
print("STAGE 3: FEATURE ENGINEERING")
print("=" * 60)

print("\nReading:")
print(INPUT_PATH)

df = spark.read.parquet(INPUT_PATH)

print("\nInput records:")
print(df.count())

print("\nInput columns:")
print(df.columns)


# ============================================================
# 7. DEPRECIATION CURVE
# ============================================================

print("\n----------------------------------------------")
print("Creating depreciation curve...")
print("----------------------------------------------")

depreciation_df = (
    df
    .filter(
        F.col("posted_date").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
    )
    .withColumn(
        "posted_year",
        F.year("posted_date")
    )
    .groupBy(
        "category",
        "posted_year"
    )
    .agg(
        F.count("*").alias("listing_count"),
        F.round(
            F.avg("price"),
            2
        ).alias("average_price"),
        F.round(
            F.min("price"),
            2
        ).alias("min_price"),
        F.round(
            F.max("price"),
            2
        ).alias("max_price")
    )
    .orderBy(
        "category",
        "posted_year"
    )
)


print("Depreciation rows:")
print(depreciation_df.count())


# ============================================================
# 8. RESALE VELOCITY
# ============================================================

print("\n----------------------------------------------")
print("Creating resale velocity...")
print("----------------------------------------------")

velocity_base = (
    df
    .filter(
        F.col("posted_date").isNotNull()
        & F.col("delisted_date").isNotNull()
    )
    .withColumn(
        "days_to_resale",
        F.datediff(
            F.col("delisted_date"),
            F.col("posted_date")
        )
    )
    .filter(
        (F.col("days_to_resale") >= 0)
        & (F.col("days_to_resale") <= 3650)
    )
)


velocity_df = (
    velocity_base
    .groupBy(
        "category",
        "source_platform"
    )
    .agg(
        F.count("*").alias("resale_count"),
        F.round(
            F.avg("days_to_resale"),
            2
        ).alias("average_days_to_resale"),
        F.round(
            F.expr("percentile_approx(days_to_resale, 0.5)"),
            2
        ).alias("median_days_to_resale"),
        F.round(
            F.min("days_to_resale"),
            2
        ).alias("min_days_to_resale"),
        F.round(
            F.max("days_to_resale"),
            2
        ).alias("max_days_to_resale")
    )
    .orderBy(
        "category",
        "source_platform"
    )
)


print("Resale velocity rows:")
print(velocity_df.count())


# ============================================================
# 9. REGIONAL PRICE VARIANCE
# ============================================================

print("\n----------------------------------------------")
print("Creating regional price variance...")
print("----------------------------------------------")

regional_df = (
    df
    .filter(
        F.col("location_region").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
    )
    .groupBy(
        "location_region",
        "category"
    )
    .agg(
        F.count("*").alias("listing_count"),
        F.round(
            F.avg("price"),
            2
        ).alias("average_price"),
        F.round(
            F.stddev("price"),
            2
        ).alias("price_stddev"),
        F.round(
            F.variance("price"),
            2
        ).alias("price_variance"),
        F.round(
            F.min("price"),
            2
        ).alias("min_price"),
        F.round(
            F.max("price"),
            2
        ).alias("max_price")
    )
    .orderBy(
        "location_region",
        "category"
    )
)


print("Regional variance rows:")
print(regional_df.count())


# ============================================================
# 10. REMOVE OLD OUTPUTS
# ============================================================

print("\nRemoving previous curated outputs if present...")

for path in [
    DEPRECIATION_PATH,
    VELOCITY_PATH,
    REGIONAL_PATH
]:
    if os.path.exists(path):
        shutil.rmtree(path)


# ============================================================
# 11. WRITE DEPRECIATION CURVE
# ============================================================

print("\nWriting:")
print(DEPRECIATION_PATH)

(
    depreciation_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(DEPRECIATION_PATH)
)


# ============================================================
# 12. WRITE RESALE VELOCITY
# ============================================================

print("\nWriting:")
print(VELOCITY_PATH)

(
    velocity_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(VELOCITY_PATH)
)


# ============================================================
# 13. WRITE REGIONAL PRICE VARIANCE
# ============================================================

print("\nWriting:")
print(REGIONAL_PATH)

(
    regional_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(REGIONAL_PATH)
)


# ============================================================
# 14. VERIFY OUTPUTS
# ============================================================

print("\n==============================================")
print("FINAL VERIFICATION")
print("==============================================")


print("\nDepreciation curve:")
dep_check = spark.read.parquet(DEPRECIATION_PATH)
print("Records:", dep_check.count())


print("\nResale velocity:")
velocity_check = spark.read.parquet(VELOCITY_PATH)
print("Records:", velocity_check.count())


print("\nRegional price variance:")
regional_check = spark.read.parquet(REGIONAL_PATH)
print("Records:", regional_check.count())


# ============================================================
# 15. SHOW SAMPLES
# ============================================================

print("\n========== DEPRECIATION SAMPLE ==========")
dep_check.show(10, truncate=False)

print("\n========== RESALE VELOCITY SAMPLE ==========")
velocity_check.show(10, truncate=False)

print("\n========== REGIONAL VARIANCE SAMPLE ==========")
regional_check.show(10, truncate=False)


# ============================================================
# 16. COMPLETE
# ============================================================

print("\n==============================================")
print("STAGE 3 FEATURE ENGINEERING COMPLETE")
print("==============================================")

print("\nOutputs:")

print(DEPRECIATION_PATH)
print(VELOCITY_PATH)
print(REGIONAL_PATH)

spark.stop()