"""
ResellRadar - acceptance checks for the Spark stage outputs (Person 1).

Checks the artifacts each pipeline stage writes against the contracts in
docs/SCHEMA_MISMATCHES.md and against properties designed into the raw zone:

    stage1  data/processed/clean_listings.parquet   <- spark_jobs/clean_normalize.py
    stage2  data/processed/entity_resolved.parquet  <- spark_jobs/entity_resolution.py
    stage3  data/curated/*_curated.parquet          <- spark_jobs/feature_engineering.py

This is the structural half of acceptance; accuracy is graded separately by
scripts/evaluate_er.py against data/ground_truth/run=*/truth.parquet.

Usage:
    python scripts/accept_pipeline.py stage1
    python scripts/accept_pipeline.py stage2
    python scripts/accept_pipeline.py stage3
    python scripts/accept_pipeline.py all --out logs/acceptance_checks.md
"""

import argparse
import glob
import json
import os
import re
import sys

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED = os.path.join(PROJECT_ROOT, "data", "processed")
CURATED = os.path.join(PROJECT_ROOT, "data", "curated")

# Raw-zone facts (logs/validation_report.md + a direct count of the generated JSONL):
# mercari train.tsv 1,482,535 rows with no null title/price/listing_id;
# generated 500,000 rows of which 9,856 carry a null `price` by design (~2% messiness).
# Manual uploads (data/raw/uploads/) join the same union, so the expected counts are
# DERIVED from the raw zone at runtime instead of hardcoded - uploading data shifts
# the expectations instead of failing the acceptance gate.
RAW_ROWS = 1_982_535
MERCARI_ROWS = 1_482_535
GENERATED_ROWS = 500_000
GENERATED_NULL_PRICE = 9_856
CLEAN_ROWS = RAW_ROWS - GENERATED_NULL_PRICE          # 1,972,679
UPLOADS_ROWS = 0
UPLOADS_NULL_PRICE = 0


def _canonical_generated_run() -> str:
    """Mirror the generated run partition that clean_normalize.py actually reads
    (GENERATED_PATH there points at ONE canonical run; later console-trigger
    runs are test batches that are not part of the union). Parsing the run id
    out of the job file keeps acceptance and Spark in sync automatically."""
    default = "run=2026_09_28T150245Z"
    try:
        with open(os.path.join(PROJECT_ROOT, "spark_jobs", "clean_normalize.py"), encoding="utf-8") as fh:
            m = re.search(r"run=(\d{4}_\d{2}_\d{2}T\d{6}Z)", fh.read())
        return f"run={m.group(1)}" if m else default
    except OSError:
        return default


def derive_raw_expectations() -> dict:
    """Count raw-zone rows per source (and null-price rows) so the expected
    pipeline numbers always match whatever the raw zone actually contains,
    including manually uploaded batches."""
    raw = os.path.join(PROJECT_ROOT, "data", "raw")
    exp = {
        "mercari": 0,
        "generated": 0,
        "uploads": 0,
        "generated_null_price": 0,
        "uploads_null_price": 0,
    }

    for tsv in glob.glob(os.path.join(raw, "mercari", "*.tsv")):
        with open(tsv, encoding="utf-8") as fh:
            exp["mercari"] += sum(1 for _ in fh) - 1  # header

    # generated: only the canonical partition the Spark stage reads
    for jl in glob.glob(os.path.join(raw, "generated", _canonical_generated_run(), "*.jsonl")):
        with open(jl, encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    exp["generated_null_price"] += 1
                    continue
                exp["generated"] += 1
                if rec.get("price") is None:
                    exp["generated_null_price"] += 1

    # uploads: every uploaded batch joins the union (that is the feature)
    for jl in glob.glob(os.path.join(raw, "uploads", "**", "*.jsonl"), recursive=True):
        with open(jl, encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    exp["uploads_null_price"] += 1  # unreadable row can never survive stage 1
                    continue
                exp["uploads"] += 1
                if rec.get("price") is None:
                    exp["uploads_null_price"] += 1
    return exp

BASE_FIELDS = [
    "listing_id", "title", "description", "price", "price_raw", "currency",
    "category", "sub_category", "category_full", "item_condition_id",
    "brand_name", "shipping", "posted_date", "delisted_date", "location_city",
    "location_region", "seller_type", "seller_id", "source_platform",
]

CURATED_TABLES = [
    "depreciation_curve_curated.parquet",
    "resale_velocity_curated.parquet",
    "regional_price_variance_curated.parquet",
]


class Checks:
    """Collects PASS/FAIL checks and prints them as they are made."""

    def __init__(self, title):
        self.title = title
        self.results = []

    def check(self, name, ok, detail=""):
        self.results.append((name, bool(ok), detail))
        line = "PASS" if ok else "FAIL"
        print(f"[{line}] {name}" + (f" -- {detail}" if detail else ""), flush=True)

    def info(self, name, detail):
        print(f"[info] {name}: {detail}", flush=True)

    @property
    def failures(self):
        return [(n, d) for n, ok, d in self.results if not ok]


def load(path, columns=None):
    if not os.path.exists(path):
        sys.exit(f"[error] missing artifact: {os.path.relpath(path, PROJECT_ROOT)}")
    return pd.read_parquet(path, columns=columns)


def check_stage1():
    print("\n=== STAGE 1 clean_listings.parquet (clean_normalize.py) ===", flush=True)
    df = load(
        os.path.join(PROCESSED, "clean_listings.parquet"),
        ["listing_id", "source_platform", "title", "price", "title_clean",
         "description", "posted_date", "delisted_date", "seller_id",
         "location_city", "brand_name", "category_full", "item_condition_id"],
    )
    c = Checks("stage1")

    c.check(
        "row count equals the raw zone minus the designed null-price rows",
        len(df) == CLEAN_ROWS,
        f"{len(df):,} rows (expected {CLEAN_ROWS:,})",
    )
    c.check(
        "no duplicate listing_id after dropDuplicates",
        not df["listing_id"].duplicated().any(),
        f"{int(df['listing_id'].duplicated().sum()):,} duplicates",
    )
    nulls = {col: int(df[col].isna().sum()) for col in ["listing_id", "title", "price", "source_platform"]}
    c.check(
        "listing_id / title / price / source_platform are never null",
        all(v == 0 for v in nulls.values()),
        str(nulls),
    )

    platforms = df["source_platform"].value_counts().to_dict()
    uploads_seen = platforms.get("uploads", 0)
    c.check(
        "all sources survived the union with the expected row counts",
        platforms.get("mercari") == MERCARI_ROWS
        and platforms.get("generated") == GENERATED_ROWS - GENERATED_NULL_PRICE
        and uploads_seen == UPLOADS_ROWS - UPLOADS_NULL_PRICE,
        f"mercari={platforms.get('mercari', 0):,} (expected {MERCARI_ROWS:,}), "
        f"generated={platforms.get('generated', 0):,} (expected {GENERATED_ROWS - GENERATED_NULL_PRICE:,}), "
        f"uploads={uploads_seen:,} (expected {UPLOADS_ROWS - UPLOADS_NULL_PRICE:,})",
    )

    mercari = df["source_platform"] == "mercari"
    generated = df["source_platform"] == "generated"
    zero_null = ["posted_date", "delisted_date", "seller_id", "location_city"]
    c.check(
        "Mercari rows carry NULL dates/location/seller_id (no such fields in real data)",
        all(int(df.loc[mercari, col].isna().sum()) == int(mercari.sum()) for col in zero_null),
        ", ".join(f"{col}={int(df.loc[mercari, col].isna().sum()):,}" for col in zero_null),
    )
    c.check(
        "generated rows keep their dates and seller_id",
        int(df.loc[generated, "posted_date"].isna().sum()) == 0
        and int(df.loc[generated, "seller_id"].isna().sum()) == 0,
        f"posted_date nulls={int(df.loc[generated, 'posted_date'].isna().sum()):,}, "
        f"seller_id nulls={int(df.loc[generated, 'seller_id'].isna().sum()):,}",
    )

    brand_null = df.loc[mercari, "brand_name"].isna().mean()
    c.info(
        "mercari brand_name null rate (documented mismatch: ~43% null in real data)",
        f"{brand_null:.2%}",
    )
    rm_rows = int((df.loc[mercari, "description"].isna()).sum())
    c.info("mercari descriptions mapped to NULL from '[rm]'", f"{rm_rows:,} rows")

    tc = df["title_clean"].dropna()
    c.check(
        "title_clean is lowercased/alphanumeric-only for every row",
        len(tc) == len(df) and bool(tc.str.match(r"^[a-z0-9 ]*$").all()),
        f"{len(tc):,} non-null of {len(df):,}",
    )
    gen_full_null = int(df.loc[generated, "category_full"].isna().sum())
    merc_full_null = int(df.loc[mercari, "category_full"].isna().sum())
    merc_full_rate = merc_full_null / int(mercari.sum())
    c.check(
        "generated rows always have category_full (never null by design)",
        gen_full_null == 0,
        f"{gen_full_null:,} nulls in generated rows",
    )
    c.check(
        "Mercari category_full nulls stay within the documented ~0.4% missing category_name",
        merc_full_rate < 0.01,
        f"{merc_full_null:,} nulls ({merc_full_rate:.2%}) in mercari rows",
    )
    return c


def check_stage2():
    print("\n=== STAGE 2 entity_resolved.parquet (entity_resolution.py) ===", flush=True)
    path = os.path.join(PROCESSED, "entity_resolved.parquet")
    df = load(path)
    c = Checks("stage2")

    required = BASE_FIELDS + ["title_clean", "description_clean", "entity_id",
                              "predicted_is_repost", "predicted_original_listing_id"]
    missing = [col for col in required if col not in df.columns]
    c.check("all required columns present (union schema + ER outputs)", not missing,
            f"missing: {missing}" if missing else f"{len(required)} columns")
    tmp_left = [col for col in ["model_key", "repost_title_key", "previous_posted_date",
                                "days_since_previous"] if col in df.columns]
    c.check("temporary ER helper columns dropped before writing", not tmp_left, str(tmp_left))

    c.check(
        "row count unchanged from stage 1 (ER must not drop listings)",
        len(df) == CLEAN_ROWS,
        f"{len(df):,} rows (expected {CLEAN_ROWS:,})",
    )
    c.check("no duplicate listing_id", not df["listing_id"].duplicated().any(),
            f"{int(df['listing_id'].duplicated().sum()):,} duplicates")
    c.check(
        "every listing received an entity_id (title -> entity mapping is total)",
        int(df["entity_id"].isna().sum()) == 0,
        f"{df['entity_id'].nunique():,} distinct entities, "
        f"{int(df['entity_id'].isna().sum()):,} nulls",
    )

    flags = set(df["predicted_is_repost"].dropna().unique().tolist())
    c.check("predicted_is_repost is a 0/1 flag", flags <= {0, 1}, f"values={sorted(flags)}")
    reposts = df[df["predicted_is_repost"] == 1]
    c.info("predicted reposts", f"{len(reposts):,} rows "
           f"({len(reposts) / len(df):.2%} of listings)")

    linked = reposts[reposts["predicted_original_listing_id"].notna()]
    c.check(
        "flagged reposts all carry a link to an earlier original",
        len(linked) == len(reposts),
        f"{len(linked):,} of {len(reposts):,} flagged rows linked",
    )
    c.check(
        "a repost never links to itself",
        not (reposts["predicted_original_listing_id"] == reposts["listing_id"]).any(),
        f"{int((reposts['predicted_original_listing_id'] == reposts['listing_id']).sum()):,} self-links",
    )
    known = set(df["listing_id"])
    dangling = int((~reposts["predicted_original_listing_id"].dropna().isin(known)).sum())
    c.check("every predicted original id exists in the dataset", dangling == 0,
            f"{dangling:,} dangling links")
    return c


def check_stage3():
    print("\n=== STAGE 3 curated tables (feature_engineering.py) ===", flush=True)
    c = Checks("stage3")
    for name in CURATED_TABLES:
        path = os.path.join(CURATED, name)
        if not os.path.exists(path):
            c.check(f"{name} written", False, "file missing")
            continue
        table = load(path)
        c.check(f"{name} written and non-empty", len(table) > 0, f"{len(table):,} rows")
        c.info(f"{name} columns", ", ".join(map(str, table.columns)))
        all_null = [str(col) for col in table.columns if table[col].isna().all()]
        c.check(f"{name} has no entirely-null column", not all_null, str(all_null))
    return c


def main():
    global RAW_ROWS, MERCARI_ROWS, GENERATED_ROWS, GENERATED_NULL_PRICE, CLEAN_ROWS
    global UPLOADS_ROWS, UPLOADS_NULL_PRICE
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["stage1", "stage2", "stage3", "all"])
    ap.add_argument("--out", default=None, help="write a markdown evidence file here")
    args = ap.parse_args()

    # Expected counts always mirror the current raw zone (incl. manual uploads).
    exp = derive_raw_expectations()
    MERCARI_ROWS = exp["mercari"]
    GENERATED_ROWS = exp["generated"]
    GENERATED_NULL_PRICE = exp["generated_null_price"]
    UPLOADS_ROWS = exp["uploads"]
    UPLOADS_NULL_PRICE = exp["uploads_null_price"]
    RAW_ROWS = MERCARI_ROWS + GENERATED_ROWS + UPLOADS_ROWS
    CLEAN_ROWS = RAW_ROWS - GENERATED_NULL_PRICE - UPLOADS_NULL_PRICE
    print(
        f"[expectations] mercari={MERCARI_ROWS:,} generated={GENERATED_ROWS:,} "
        f"uploads={UPLOADS_ROWS:,} | null-price drops={GENERATED_NULL_PRICE + UPLOADS_NULL_PRICE:,} "
        f"| expected clean={CLEAN_ROWS:,}",
        flush=True,
    )

    runners = {"stage1": check_stage1, "stage2": check_stage2, "stage3": check_stage3}
    chosen = list(runners) if args.stage == "all" else [args.stage]

    results = []
    for stage in chosen:
        results.append((stage, runners[stage]()))

    print("\n========== SUMMARY ==========")
    failed_total = 0
    for stage, c in results:
        failed = c.failures
        failed_total += len(failed)
        print(f"{stage}: {len(c.results) - len(failed)}/{len(c.results)} checks passed")
        for name, detail in failed:
            print(f"    FAILED: {name} -- {detail}")

    if args.out:
        out_path = os.path.join(PROJECT_ROOT, args.out)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write("# ResellRadar pipeline acceptance checks\n\n")
            for stage, c in results:
                fh.write(f"## {stage}\n\n")
                for name, ok, detail in c.results:
                    mark = "PASS" if ok else "FAIL"
                    fh.write(f"- [{mark}] {name}" + (f" -- {detail}" if detail else "") + "\n")
                fh.write("\n")

    print(f"\nRESULT: {'ALL CHECKS PASSED' if failed_total == 0 else str(failed_total) + ' CHECK(S) FAILED'}")
    return 1 if failed_total else 0


if __name__ == "__main__":
    sys.exit(main())
