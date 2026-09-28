"""
ResellRadar — QA profile for generated JSONL batches (Person 1).

Quick statistical check of a generated run against the messiness/signal targets
in docs/SCHEMA_generated.md. Used for the 10k smoke test and the full-scale run.

Usage:
    python scripts/qa_generated.py --run-dir data/raw/generated/run=2026_09_28T...Z [--sample 200000]
"""

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRICE_FMT_RE = re.compile(r"^\$[\d,]+(?:\.\d+)?k?/-?$|^\$[\d,]+(?:\.\d+)?k?$|^\d+(?:\.\d+)?$")


def classify_price_raw(s: str) -> str:
    if s == "":
        return "blank"
    if s == "negotiable":
        return "negotiable"
    if s.endswith("/-"):
        return "dollar_slash"
    if s.endswith("k") or s.endswith("K"):
        return "k_suffix"
    if s.startswith("$"):
        return "dollar"
    return "plain"


def load_run(run_dir: str, sample: int | None) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(run_dir, "*.jsonl")))
    if not files:
        sys.exit(f"[error] no .jsonl files under {run_dir}")
    dfs = []
    for f in files:
        dfs.append(pd.read_json(f, lines=True))
    df = pd.concat(dfs, ignore_index=True)
    if sample and len(df) > sample:
        df = df.sample(n=sample, random_state=42).reset_index(drop=True)
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--truth", default=None, help="path to truth.parquet (default: auto-detect by run name)")
    ap.add_argument("--sample", type=int, default=None, help="cap rows for fast profiling")
    args = ap.parse_args()

    t0 = datetime.now()
    df = load_run(args.run_dir, args.sample)
    print(f"[load] {len(df):,} rows from {args.run_dir} ({(datetime.now() - t0).total_seconds():.1f}s)")

    # ground truth (sibling partition under data/ground_truth*/)
    run_name = os.path.basename(os.path.normpath(args.run_dir))
    truth_path = args.truth
    if truth_path is None:
        for base in ("data/ground_truth", "data/ground_truth_smoke"):
            cand = os.path.join(PROJECT_ROOT, base, run_name, "truth.parquet")
            if os.path.exists(cand):
                truth_path = cand
                break
    truth = None
    if truth_path:
        truth = pd.read_parquet(truth_path)
        print(f"[load] ground truth: {os.path.relpath(truth_path, PROJECT_ROOT)} ({len(truth):,} rows)")

    checks = []

    def check(name, value, ok):
        checks.append((name, value, ok))

    # --- messiness targets ---
    nulls = {
        "description": df["description"].isna().mean(),
        "sub_category": df["sub_category"].isna().mean(),
        "delisted_date": df["delisted_date"].isna().mean(),
        "price": df["price"].isna().mean(),
    }
    for k, v in nulls.items():
        target = (0.03, 0.07) if k in ("description", "sub_category") else None
        check(f"null rate: {k}", f"{v:.1%}", True if target is None else target[0] <= v <= target[1])

    fmts = df["price_raw"].astype(str).map(classify_price_raw).value_counts(normalize=True)
    check("price_raw formats (>=4 distinct)", json.dumps({k: f"{v:.1%}" for k, v in fmts.items()}),
          len(fmts) >= 4)
    check("blank+negotiable share ~2%", f"{nulls['price']:.1%}", 0.015 <= nulls["price"] <= 0.025)

    # --- catalog size: >= 1,000 canonical products by construction ---
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "generator"))
    from generate_data import canonical_catalog
    catalog = canonical_catalog()
    check("canonical product count >= 1,000", f"{len(catalog['ids']):,} built",
          len(catalog["ids"]) >= 1_000)
    if truth is not None:
        realized = truth["true_canonical_id"].nunique()
        # small runs realize a fraction of the catalog; full 500k realizes ~all of it
        realized_floor = min(1_000, max(50, len(truth) // 12))
        check("distinct canonicals realized in run", f"{realized:,} (floor {realized_floor:,})",
              realized >= realized_floor)

    if truth is not None:
        # title variants per canonical, measured exactly via ground truth
        per_canon = truth.groupby("true_canonical_id").size()
        attended = per_canon[per_canon >= max(8, len(truth) // 25_000)]
        joined = truth.merge(df[["listing_id", "title"]], on="listing_id")
        uniq = joined.groupby("true_canonical_id")["title"].nunique()
        uniq_att = uniq[attended.index]
        check("title variants per canonical (6-15 design pool)",
              f"canonicals measured={len(uniq_att)}, min={int(uniq_att.min())}, "
              f"median={int(uniq_att.median())}, max={int(uniq_att.max())}",
              # small runs sample few rows per canonical; require the pool to be
              # visible in the median and a small-sample floor on the minimum
              len(uniq_att) >= 3 and int(uniq_att.min()) >= 3 and int(uniq_att.median()) >= 8)

    alias_share = df["location_city"].isin(["NYC", "SF", "LA", "Philly"]).mean()
    check("city alias share", f"{alias_share:.1%}", alias_share > 0.10)

    # outliers: injected outliers are exactly 0.1x / 8x the pre-outlier price, so
    # flag prices far from their (brand, condition) cell median; the cell absorbs
    # the structural spread (age, city, noise), leaving the injections exposed.
    p = df["price"].dropna()
    cell = (df.loc[p.index, "brand_name"].fillna("?") + "|"
            + df.loc[p.index, "item_condition_id"].astype(str))
    cell_med = p.groupby(cell).transform("median")
    ratio = p / cell_med
    outlier_share = float(((ratio < 0.2) | (ratio > 5)).mean())
    check("outlier price share (~1%)", f"{outlier_share:.2%}", 0.005 <= outlier_share <= 0.02)

    # --- signal checks ---
    posted = pd.to_datetime(df["posted_date"])
    span_days = (posted.max() - posted.min()).days
    check("posted_date span ~24 months (730d)", f"{span_days} days", 650 <= span_days <= 735)

    dl = df["delisted_date"].dropna()
    ok_order = bool((pd.to_datetime(dl) >= posted[dl.index]).all())
    check("delisted_date >= posted_date (all)", ok_order, ok_order)

    tts = (pd.to_datetime(dl) - posted[dl.index]).dt.days
    check("time-to-sale median ~28d (lognormal tail)",
          f"median {tts.median():.0f}d, p90 {tts.quantile(0.9):.0f}d, max {tts.max()}d",
          20 <= tts.median() <= 40)

    # age vs price: Pearson on the injected signal (log price vs age should slope down)
    age_months = 24 - (posted - posted.min()).dt.days / 30.44
    tmp = pd.DataFrame({"age": age_months, "price": df["price"]}).dropna()
    slope = np.corrcoef(tmp["age"], np.log1p(tmp["price"]))[0, 1]
    check("price declines with age (corr < -0.15)", f"corr(log price, age) = {slope:.3f}", slope < -0.15)

    region_med = df.groupby("location_region")["price"].median()
    spread = region_med.max() / region_med.min()
    check("regional price spread exists", f"{spread:.2f}x (max/min median)", spread > 1.15)

    cond_med = df.groupby("item_condition_id")["price"].median()
    check("condition 5 median > condition 1 median",
          f"c1={cond_med[1]:.0f} c5={cond_med[5]:.0f}", cond_med[5] > cond_med[1])

    brand_med = df.groupby("brand_name")["price"].median().sort_values()
    check("brand price separation", f"{dict(brand_med.round(0))}", brand_med.iloc[-1] > 2 * brand_med.iloc[0])

    if truth is not None:
        # designed repost share (~10%) straight from ground truth
        rep_share = float(truth["is_repost"].mean())
        check("designed repost share ~10% (8-12% band)", f"{rep_share:.1%}",
              0.08 <= rep_share <= 0.12)

        # every repost shares seller_id with its original, is posted 1-14 days
        # later, and has a near-identical title (original + small suffix)
        m = df[["listing_id", "seller_id", "title", "posted_date"]].merge(truth, on="listing_id")
        reps = m[m["is_repost"]]
        orig = m[["listing_id", "seller_id", "posted_date", "title"]].rename(
            columns={"listing_id": "original_listing_id", "seller_id": "orig_seller",
                     "posted_date": "orig_posted", "title": "orig_title"})
        j = reps.merge(orig, on="original_listing_id", how="left")
        same_seller = float((j["seller_id"] == j["orig_seller"]).mean())
        gap = (pd.to_datetime(j["posted_date"]) - pd.to_datetime(j["orig_posted"])).dt.days
        gaps_ok = bool(gap.between(1, 14).all())
        def _norm(s: pd.Series) -> pd.Series:
            return s.astype(str).str.replace(r"( - must go| \(relist\)|!)+$", "", regex=True)
        title_same = float((_norm(j["title"]) == _norm(j["orig_title"])).mean())
        check("reposts share seller_id with original (100%)", f"{same_seller:.1%}", same_seller == 1.0)
        check("repost posted_date gap within 1-14 days", f"min {gap.min()}, max {gap.max()}", gaps_ok)
        check("repost titles near-identical to original", f"{title_same:.1%}", title_same == 1.0)
        orphans = int((~j["original_listing_id"].isin(df["listing_id"])).sum())
        check("no dangling original_listing_id links", f"{orphans}", orphans == 0)
    else:
        check("ground truth file found", "NOT FOUND - truth-based checks skipped", False)

    # seller pool sanity (skew: top sellers own a visible share of listings)
    if "seller_id" in df:
        vc = df["seller_id"].value_counts(normalize=True)
        check("seller pool active + skewed",
              f"{df['seller_id'].nunique():,} distinct sellers, top-1 {vc.iloc[0]:.2%}, top-10 {vc.head(10).sum():.1%}",
              df["seller_id"].nunique() > 1_000 and vc.iloc[0] > 0.005)

    check("category split", df["category"].value_counts(normalize=True).round(3).to_dict(), True)
    check("category_full never null", f"{df['category_full'].isna().mean():.1%}", df["category_full"].notna().all())
    check("listing_id unique", f"{df['listing_id'].nunique():,}", df["listing_id"].is_unique)

    # --- report ---
    print("\n=== QA CHECKS ===")
    failures = 0
    for name, value, ok in checks:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            failures += 1
        print(f"[{mark}] {name}: {value}")
    print(f"\n{len(checks) - failures}/{len(checks)} checks passed in "
          f"{(datetime.now() - t0).total_seconds():.1f}s")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
