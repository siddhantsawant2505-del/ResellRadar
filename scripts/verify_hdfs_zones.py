"""
ResellRadar - read-back proof that the HDFS zones are real, readable tables.

Run through the Spark container (scripts/run_stage_in_docker.sh), which joins the compose
network and can resolve hdfs://namenode:9000 - the Windows host cannot. Reads every
processed/curated zone from HDFS and prints row/column counts, then exits non-zero if any
zone is missing or unreadable. Counts come from full table scans, so this doubles as a
blocks-are-healthy check (a corrupt/under-replicated block fails the read).

Usage:
    bash scripts/run_stage_in_docker.sh scripts/verify_hdfs_zones.py
"""

import os
import sys

from pyspark.sql import SparkSession

ZONES = [
    ("processed", "clean_listings", "hdfs://namenode:9000/data/processed/clean_listings.parquet"),
    ("processed", "entity_resolved", "hdfs://namenode:9000/data/processed/entity_resolved.parquet"),
    ("curated", "depreciation_curve", "hdfs://namenode:9000/data/curated/depreciation_curve_curated.parquet"),
    ("curated", "resale_velocity", "hdfs://namenode:9000/data/curated/resale_velocity_curated.parquet"),
    ("curated", "regional_price_variance", "hdfs://namenode:9000/data/curated/regional_price_variance_curated.parquet"),
]

spark = (
    SparkSession.builder
    .appName("ResellRadar-VerifyHdfszones")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("========== HDFS ZONE READ-BACK ==========")
failures = 0
for zone, name, path in ZONES:
    try:
        df = spark.read.parquet(path)
        rows = df.count()
        cols = len(df.columns)
        print(f"[OK]   /{zone}/{name:<26} {rows:>9,} rows x {cols} cols")
    except Exception as exc:
        failures += 1
        msg = str(exc).splitlines()[0][:120] if str(exc) else "unknown error"
        print(f"[FAIL] /{zone}/{name}: {msg}")

print("-----------------------------------------")
if failures:
    print(f"RESULT: {failures} zone(s) FAILED read-back")
    spark.stop()
    sys.exit(1)

print("RESULT: all zones readable from hdfs://namenode:9000")
spark.stop()
