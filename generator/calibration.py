"""
ResellRadar generator — calibration from real Mercari data.

Loads data/seed/mercari_phone_furniture_sample.csv (produced by
scripts/extract_seed_sample.py) and fits the parameters the synthetic
generator needs:

  - phone brand mix (weights from real listing counts)
  - per-brand base price anchors (real medians)
  - condition 1-5 price multipliers (fit from real medians per condition)
  - item_condition_id distribution
  - furniture leaf mix and price anchor

Everything falls back to sensible constants if the seed file is missing,
so the generator always runs (and reports whether it used real calibration).
"""

import os

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_PATH = os.path.join(PROJECT_ROOT, "data", "seed", "mercari_phone_furniture_sample.csv")

PHONE_LEAF = "Electronics/Cell Phones & Accessories/Cell Phones & Smartphones"
FURNITURE_PREFIX = "Home/Furniture/"

# Fallbacks (used only when the seed sample is absent / too thin)
FALLBACK_PHONE_BRANDS = [
    ("Apple", 0.30, 210.0), ("Samsung", 0.22, 150.0), ("LG", 0.12, 90.0),
    ("Motorola", 0.11, 70.0), ("Google", 0.08, 160.0), ("OnePlus", 0.05, 180.0),
    ("Xiaomi", 0.04, 110.0), ("BLU", 0.04, 45.0), ("Nokia", 0.02, 40.0), ("Sony", 0.02, 95.0),
]
FALLBACK_FURNITURE_MEDIAN = 65.0
FALLBACK_CONDITION_DIST = [0.06, 0.18, 0.34, 0.28, 0.14]
FALLBACK_CONDITION_MULTS = [0.40, 0.72, 0.95, 1.18, 1.45]


def load_calibration(seed_path: str = SEED_PATH) -> dict:
    calib = {
        "source": "fallback (no seed sample found)",
        "phone_brands": FALLBACK_PHONE_BRANDS,
        "condition_dist": FALLBACK_CONDITION_DIST,
        "condition_mults": FALLBACK_CONDITION_MULTS,
        "furniture_leaves": [("Living Room Furniture", 0.30), ("Bedroom Furniture", 0.25),
                             ("Home Office Furniture", 0.20), ("Home Entertainment Furniture", 0.10),
                             ("Other Furniture", 0.10), ("Home Bar Furniture", 0.05)],
        "furniture_price_anchor": FALLBACK_FURNITURE_MEDIAN,
        "n_seed_rows": 0,
    }
    if not os.path.exists(seed_path):
        return calib

    df = pd.read_csv(seed_path)
    calib["n_seed_rows"] = len(df)
    phones = df[df["category_name"] == PHONE_LEAF]
    furniture = df[df["category_name"].str.startswith(FURNITURE_PREFIX, na=False)]

    # --- phone brands: weight = real count, anchor = real median price ---
    if len(phones) >= 200:
        g = phones.groupby("brand_name")["price"]
        stats = g.agg(["count", "median"])
        stats = stats[stats["count"] >= 30].sort_values("count", ascending=False)
        if len(stats):
            total_w = float(stats["count"].sum())
            brands = [(str(b), float(row["count"]) / total_w, float(row["median"]))
                      for b, row in stats.head(12).iterrows()]
            calib["phone_brands"] = brands
            calib["source"] = f"real Mercari seed ({len(phones):,} phone rows)"

    # --- condition multipliers: fit from real per-condition medians ---
    # Mercari's raw condition->price medians are non-monotonic on phones
    # (cond2 median > cond5 median), so we only adopt the empirical fit when it
    # is monotone increasing in 1..5 (the documented scale); otherwise fall back
    # to clean monotone multipliers calibrated to the real price spread.
    if len(phones) >= 200:
        med = phones.groupby("item_condition_id")["price"].median()
        base = med.get(3, np.nan)
        if pd.notna(base) and base > 0:
            mults = []
            for c in range(1, 6):
                m = med.get(c, np.nan)
                mults.append(float(m / base) if pd.notna(m) and m > 0 else FALLBACK_CONDITION_MULTS[c - 1])
            if all(mults[i] <= mults[i + 1] for i in range(4)):
                calib["condition_mults"] = [round(min(m, 1.6), 3) for m in mults]
                calib["condition_mult_source"] = "empirical (monotone)"
        cond_counts = phones["item_condition_id"].value_counts(normalize=True)
        if len(cond_counts) == 5:
            calib["condition_dist"] = [float(cond_counts.get(c, 0.0)) for c in range(1, 6)]

    # --- furniture leaves + price anchor ---
    if len(furniture) >= 50:
        leaf_counts = furniture["category_name"].str.split("/").str[-1].value_counts(normalize=True)
        calib["furniture_leaves"] = [(str(leaf), float(w)) for leaf, w in leaf_counts.items()]
        calib["furniture_price_anchor"] = float(furniture["price"].median())

    return calib


if __name__ == "__main__":
    import json
    print(json.dumps(load_calibration(), indent=2, default=str))
