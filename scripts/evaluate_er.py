"""
ResellRadar - ER evaluation harness (Person 1).

Grades an entity-resolution output file against the generator's ground truth:
    data/ground_truth/run=<ts>/truth.parquet
        listing_id, true_canonical_id, is_repost, original_listing_id

Predictions file (.parquet or .csv) must contain:
    listing_id
    predicted_canonical_id        (aliases: pred_canonical_id, canonical_id, cluster_id,
                                   entity_id)
and optionally:
    predicted_original_listing_id (aliases: pred_original_listing_id, original_id)
    predicted_is_repost           (aliases: pred_is_repost, repost_pred)

Metrics
    1. Canonical clustering (task A): pairwise precision / recall / F1 of
       same-canonical decisions, computed exactly from cluster sizes
       (pairs = sum n_i*(n_i-1)/2) - no O(n^2) expansion.
    2. Repost detection (task B): binary P/R/F1 on is_repost when the prediction
       column exists; otherwise derived from predicted cluster size > 1.
    3. Link recovery (task C, optional): exact-match P/R of predicted original
       listing ids among the truth reposts.

Usage:
    python scripts/evaluate_er.py --pred <predictions.parquet|csv> [--truth path] [--out logs/er_report.md]
"""

import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# `entity_id` is what spark_jobs/entity_resolution.py actually emits; accepted as an alias
# so the delivered Stage 2 output grades without a rename step.
CANON_ALIASES = ["predicted_canonical_id", "pred_canonical_id", "canonical_id", "cluster_id",
                 "entity_id"]
ORIG_ALIASES = ["predicted_original_listing_id", "pred_original_listing_id", "original_id"]
REPOST_ALIASES = ["predicted_is_repost", "pred_is_repost", "repost_pred"]


def latest_truth() -> str:
    runs = sorted(glob.glob(os.path.join(PROJECT_ROOT, "data", "ground_truth", "run=*", "truth.parquet")))
    if not runs:
        sys.exit("[error] no truth.parquet under data/ground_truth/run=/")
    return runs[-1]


def load_predictions(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path, dtype=str)
    df = df.rename(columns={c: c.strip() for c in df.columns})
    if "listing_id" not in df.columns:
        sys.exit("[error] predictions file must have a 'listing_id' column")
    canon = next((c for c in CANON_ALIASES if c in df.columns), None)
    if canon is None:
        sys.exit(f"[error] predictions file needs one of: {', '.join(CANON_ALIASES)}")
    df = df.rename(columns={canon: "predicted_canonical_id"})
    orig = next((c for c in ORIG_ALIASES if c in df.columns), None)
    if orig:
        df = df.rename(columns={orig: "predicted_original_listing_id"})
    rep = next((c for c in REPOST_ALIASES if c in df.columns), None)
    if rep:
        df = df.rename(columns={rep: "predicted_is_repost"})
    return df


def pair_counts(labels: pd.Series) -> tuple:
    """(within-pairs, total) for a labeling; nulls count as singleton clusters."""
    sizes = labels.dropna().value_counts()
    within = int((sizes * (sizes - 1) // 2).sum())
    n = len(labels)
    total = n * (n - 1) // 2
    return within, total


def prf(tp: int, pred: int, truth: int) -> tuple:
    p = tp / pred if pred else 0.0
    r = tp / truth if truth else 0.0
    f = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True, help="predictions .parquet or .csv")
    ap.add_argument("--truth", default=None, help="truth.parquet (default: latest run)")
    ap.add_argument("--out", default=None, help="also write the report to this .md path")
    args = ap.parse_args()

    truth_path = args.truth or latest_truth()
    truth = pd.read_parquet(truth_path)
    pred = load_predictions(args.pred)
    print(f"[load] truth : {os.path.relpath(truth_path, PROJECT_ROOT)} ({len(truth):,} rows)")
    print(f"[load] pred  : {args.pred} ({len(pred):,} rows)")

    lines = [f"# ResellRadar ER evaluation", "",
             f"Truth: `{os.path.relpath(truth_path, PROJECT_ROOT)}`  |  "
             f"Predictions: `{args.pred}`", ""]

    # --- coverage / integrity ---
    dup_ids = int(pred["listing_id"].duplicated().sum())
    merged = truth.merge(pred, on="listing_id", how="left", indicator=True)
    missing = int((merged["_merge"] == "left_only").sum())
    coverage = 1.0 - missing / len(truth)
    lines += ["## Coverage & integrity", "",
              f"- predictions rows: {len(pred):,} (truth {len(truth):,})",
              f"- duplicate listing_ids in predictions: {dup_ids:,}",
              f"- truth listings with no prediction: {missing:,} (coverage {coverage:.2%})", ""]
    if dup_ids:
        lines.append("> WARN: duplicated listing_ids - metrics computed on the first occurrence.\n")
        pred = pred.drop_duplicates("listing_id", keep="first")
        merged = truth.merge(pred, on="listing_id", how="left", indicator=True)

    unpred = merged["predicted_canonical_id"].isna()

    # --- Task A: canonical clustering (pairwise, exact via cluster sizes) ---
    pred_pairs, _ = pair_counts(merged["predicted_canonical_id"].where(~unpred))
    truth_pairs, _ = pair_counts(merged["true_canonical_id"])
    # intersection = pairs predicted together AND truly same canonical
    # Correct pairs = pairs the model puts together that are truly same-canonical. Restrict
    # to rows that HAVE a prediction: with dropna=False the unpredicted rows formed a phantom
    # NaN cluster whose within-cluster pairs were counted as correct (precision > 1).
    both = merged.loc[~unpred, ["predicted_canonical_id", "true_canonical_id"]]
    grp = both.groupby(["predicted_canonical_id", "true_canonical_id"]).size()
    tp_pairs = int((grp * (grp - 1) // 2).sum())
    pA, rA, fA = prf(tp_pairs, pred_pairs, truth_pairs)
    lines += ["## Task A - canonical clustering (pairwise)", "",
              f"- true same-canonical pairs : {truth_pairs:,}",
              f"- predicted same-cluster pairs: {pred_pairs:,}",
              f"- correct pairs (intersection): {tp_pairs:,}",
              f"- precision {pA:.4f} | recall {rA:.4f} | **F1 {fA:.4f}**", ""]

    # --- Task B: repost detection ---
    # NOTE: never derive from canonical cluster size - product clusters legitimately
    # contain hundreds of listings. A repost flag means "shares a duplicate group",
    # so only an explicit column or a predicted original link can define it.
    if "predicted_is_repost" in merged.columns:
        pred_rep = merged["predicted_is_repost"].fillna(False).astype(bool)
        src = "predicted_is_repost column"
    elif "predicted_original_listing_id" in merged.columns:
        pred_rep = merged["predicted_original_listing_id"].notna()
        src = "derived from predicted_original_listing_id presence"
    else:
        pred_rep = None
        src = None
    if pred_rep is not None:
        t_rep = merged["is_repost"].astype(bool)
        tpB = int((pred_rep & t_rep).sum())
        pB, rB, fB = prf(tpB, int(pred_rep.sum()), int(t_rep.sum()))
        lines += ["## Task B - repost detection", "",
                  f"- source of repost flag: {src}",
                  f"- truth reposts {int(t_rep.sum()):,} | flagged {int(pred_rep.sum()):,} | hits {tpB:,}",
                  f"- precision {pB:.4f} | recall {rB:.4f} | **F1 {fB:.4f}**", ""]
    else:
        lines += ["## Task B - repost detection", "",
                  "- not scored: predictions provide neither predicted_is_repost nor"
                  " predicted_original_listing_id, and canonical cluster size is not a valid"
                  " repost signal (product clusters are large by design).", ""]

    # --- Task C: original-link recovery (only if predictions provide links) ---
    if "predicted_original_listing_id" in merged.columns:
        rep_rows = merged[t_rep]
        has_pred_link = rep_rows["predicted_original_listing_id"].notna()
        correct = (rep_rows["predicted_original_listing_id"] == rep_rows["original_listing_id"])
        tpC = int((has_pred_link & correct).sum())
        pC, rC, fC = prf(tpC, int(has_pred_link.sum()), len(rep_rows))
        lines += ["## Task C - repost -> original link recovery", "",
                  f"- truth reposts: {len(rep_rows):,} | links predicted: {int(has_pred_link.sum()):,} | exact matches: {tpC:,}",
                  f"- precision {pC:.4f} | recall {rC:.4f} | **F1 {fC:.4f}**", ""]
    else:
        lines += ["## Task C - repost -> original link recovery", "",
                  "- not scored: no predicted-original column in the predictions file.", ""]

    report = "\n".join(lines)
    print(report)
    if args.out:
        os.makedirs(os.path.dirname(os.path.join(PROJECT_ROOT, args.out)), exist_ok=True)
        with open(os.path.join(PROJECT_ROOT, args.out), "w", encoding="utf-8") as f:
            f.write(report)
        print(f"[report] -> {args.out}")


if __name__ == "__main__":
    main()
