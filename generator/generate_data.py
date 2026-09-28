"""
ResellRadar - Task 3: synthetic listing generator (Person 1).

Produces ~500,000 phone + furniture listings in chunks of 10,000, written as
JSON Lines to  data/raw/generated/run=<UTC ts>/gen_YYYY_MM_DD_batchNNN.jsonl,
plus a GROUND-TRUTH file (outside the raw zone) at
data/ground_truth/run=<same ts>/truth.parquet.

Design constraints
------------------
- VECTORIZED: all random draws, prices, dates and strings are built with
  NumPy/pandas array operations per chunk. No row-by-row loops, no Faker.
- SEEDED: each batch uses rng = default_rng(seed + batch_index), so batches
  are reproducible AND resumable without changing earlier files.
- IMMUTABLE: batches are never overwritten; re-runs land in a new run= partition.
- CALIBRATED: brand mix, price anchors and the condition distribution come from
  the real Mercari seed sample via generator/calibration.py.

Canonical catalog (1,000+ canonical products)
---------------------------------------------
- phones:     brand x model x storage x color       (colors shared palette)
- furniture:  leaf x type x material x size
The canonical id (e.g. "P:Apple|iPhone 13|128GB|Midnight Black") is written ONLY
to the ground-truth parquet - never into the raw JSONL.

Sellers & reposts
-----------------
- seller_id from a 50,000-seller pool with Zipf-like skew (power sellers post
  many listings; the long tail posts few).
- ~10% of rows are designed reposts: same seller_id as the original, near-
  identical title (original title + small suffix), posted 1-14 days later,
  price perturbed +/-5%. The (is_repost, original_listing_id) link lives only
  in the ground-truth file.

Signal encoded
--------------
- posted_date spread over 24 months; price declines with item age (2.5%/month)
  + lognormal noise; delisted_date ~ lognormal time-to-sale (median 28 days);
  regional price multipliers; storage/material/size price effects.

Usage
-----
    python generator/generate_data.py --rows 10000        # smoke test
    python generator/generate_data.py                     # full 500k run
"""

import argparse
import os
import zlib
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from calibration import FALLBACK_PHONE_BRANDS, load_calibration

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUT = os.path.join(PROJECT_ROOT, "data", "raw", "generated")
DEFAULT_TRUTH = os.path.join(PROJECT_ROOT, "data", "ground_truth")
SEED = 42
CHUNK_SIZE = 10_000
REPOST_RATE = 0.10
HORIZON_DAYS = 730          # 24 months
ACTIVE_RATE = 0.18          # share of listings still up (delisted_date null)
SOLD_MISSING_RATE = 0.04    # extra nulls among sold listings
NULL_DESC = 0.05
NULL_SUBCAT = 0.05
NULL_PRICE = 0.02           # blank / "negotiable"
OUTLIER_RATE = 0.01
ALIAS_SHARE = 0.5           # share of aliased-market rows using the alias
TTS_MEDIAN_DAYS = 28.0
TTS_SIGMA = 0.9
AGE_DECLINE_PER_MONTH = 0.025
PRICE_FLOOR = 25.0

N_SELLERS = 50_000
SELLER_ZIPF_S = 0.8         # w_r ~ r^-0.8: top sellers post thousands, tail posts few
FALLBACK_BRAND_SHARE = 0.025  # total sampling mass for brands below the seed-sample threshold

# (canonical city, region, alias or None, weight, regional price multiplier)
MARKETS = [
    ("New York", "NY", "NYC", 0.14, 1.12), ("Los Angeles", "CA", "LA", 0.12, 1.08),
    ("San Francisco", "CA", "SF", 0.07, 1.18), ("Chicago", "IL", None, 0.07, 1.00),
    ("Austin", "TX", None, 0.06, 1.02), ("Dallas", "TX", None, 0.06, 0.97),
    ("Houston", "TX", None, 0.05, 0.96), ("Philadelphia", "PA", "Philly", 0.05, 0.99),
    ("Miami", "FL", None, 0.05, 1.04), ("Atlanta", "GA", None, 0.05, 0.98),
    ("Phoenix", "AZ", None, 0.04, 0.95), ("Seattle", "WA", None, 0.04, 1.06),
    ("Boston", "MA", None, 0.04, 1.09), ("Denver", "CO", None, 0.03, 1.03),
    ("Portland", "OR", None, 0.03, 1.01), ("Columbus", "OH", None, 0.02, 0.93),
    ("Charlotte", "NC", None, 0.02, 0.94), ("Nashville", "TN", None, 0.02, 0.96),
    ("Indianapolis", "IN", None, 0.02, 0.92), ("Minneapolis", "MN", None, 0.02, 0.97),
]

PHONE_MODELS = {
    "Apple": [("iPhone 15 Pro Max", ["256GB", "512GB", "1TB"]), ("iPhone 15 Pro", ["128GB", "256GB", "512GB"]),
              ("iPhone 15", ["128GB", "256GB"]), ("iPhone 14 Pro", ["128GB", "256GB", "512GB"]),
              ("iPhone 14", ["128GB", "256GB"]), ("iPhone 13", ["128GB", "256GB"]),
              ("iPhone 13 mini", ["128GB"]), ("iPhone 12", ["64GB", "128GB", "256GB"]),
              ("iPhone 11", ["64GB", "128GB"]), ("iPhone SE (2nd gen)", ["64GB", "128GB"]),
              ("iPhone SE (3rd gen)", ["64GB", "128GB", "256GB"]), ("iPhone XR", ["64GB", "128GB"]),
              ("iPhone XS", ["64GB", "256GB"]), ("iPhone 8", ["64GB", "128GB"]), ("iPhone 7", ["32GB", "128GB"])],
    "Samsung": [("Galaxy S24 Ultra", ["256GB", "512GB"]), ("Galaxy S24", ["128GB", "256GB"]),
                ("Galaxy S23", ["128GB", "256GB"]), ("Galaxy S22", ["128GB", "256GB"]),
                ("Galaxy S21", ["128GB", "256GB"]), ("Galaxy Note 20", ["128GB", "256GB"]),
                ("Galaxy Z Flip 5", ["256GB", "512GB"]), ("Galaxy Z Fold 5", ["256GB", "512GB"]),
                ("Galaxy A54", ["128GB", "256GB"]), ("Galaxy A14", ["64GB", "128GB"]),
                ("Galaxy S20 FE", ["128GB"]), ("Galaxy S10", ["128GB", "512GB"])],
    "Google": [("Pixel 9 Pro", ["128GB", "256GB", "512GB"]), ("Pixel 8", ["128GB", "256GB"]),
               ("Pixel 7a", ["128GB"]), ("Pixel 6a", ["128GB"])],
    "Xiaomi": [("Xiaomi 14", ["256GB", "512GB"]), ("Redmi Note 13", ["128GB", "256GB"]),
               ("Poco X6", ["256GB"]), ("Xiaomi 13T", ["256GB"])],
    "LG": [("Velvet", ["128GB"]), ("V60 ThinQ", ["128GB"]), ("Stylo 6", ["64GB"]),
           ("K51", ["32GB"]), ("G8 ThinQ", ["128GB"])],
    "Motorola": [("Moto G Power", ["64GB", "128GB"]), ("Moto G Stylus", ["128GB", "256GB"]),
                 ("Edge+", ["256GB"]), ("Razr 5G", ["256GB"]), ("One 5G Ace", ["64GB", "128GB"])],
    "Nokia": [("G100", ["64GB"]), ("C100", ["32GB"]), ("225 5G", ["64GB"])],
}

COLORS = ["Midnight Black", "Pearl White", "Space Gray", "Ocean Blue",
          "Titanium", "Coral Red", "Lavender", "Forest Green"]

FURNITURE_TYPES = {
    "Living Room Furniture": [("Sectional Sofa", 1.7), ("Leather Sofa", 1.6), ("Fabric Loveseat", 0.9),
                              ("Coffee Table", 0.6), ("TV Stand", 0.6), ("Recliner", 0.9),
                              ("Accent Chair", 0.6), ("Bookshelf", 0.5)],
    "Bedroom Furniture": [("Queen Bed Frame", 1.5), ("King Bed Frame", 1.7), ("Dresser", 1.0),
                          ("Nightstand", 0.5), ("Wardrobe", 1.2), ("Chest of Drawers", 0.9)],
    "Home Office Furniture": [("Standing Desk", 1.3), ("Office Chair", 0.8), ("Filing Cabinet", 0.5),
                              ("Bookcase", 0.6), ("L-Shaped Desk", 1.2)],
    "Home Entertainment Furniture": [("Entertainment Center", 1.2), ("Media Console", 0.8)],
    "Home Bar Furniture": [("Bar Cabinet", 1.1), ("Bar Stool Set", 0.6), ("Wine Rack", 0.4)],
    "Other Furniture": [("Storage Ottoman", 0.5), ("Entryway Bench", 0.6), ("Coat Rack", 0.3),
                        ("Folding Table", 0.4)],
}

FURNITURE_MATERIALS = [("Solid Wood", 1.15), ("Engineered Wood", 0.90), ("Metal", 1.00),
                       ("Fabric", 0.95), ("Leather", 1.20)]
FURNITURE_SIZES = [("Compact", 0.85), ("Standard", 1.00), ("Large", 1.15)]

STORAGE_MULT = {"32GB": 0.85, "64GB": 0.90, "128GB": 1.00, "256GB": 1.10, "512GB": 1.25, "1TB": 1.40}

FURNITURE_BRANDS = [("IKEA", 0.35), ("Ashley", 0.20), ("Wayfair", 0.15),
                    ("West Elm", 0.10), ("Unbranded", 0.20)]

SELLER_TYPES = [("Individual", 0.70), ("PowerSeller", 0.12), ("Refurbisher", 0.10), ("Liquidator", 0.08)]
COND_WORDS = ["For Parts", "Poor", "Fair", "Good", "Like New"]
COND_SENTENCES = ["Screen cracked, sold as-is.", "Heavy wear, fully functional.",
                  "Some scratches, works well.", "Light wear, everything works.",
                  "Like new, no marks."]
DESC_EXTRAS = ["Original box included.", "Charger included.", "Cash on pickup preferred.",
               "Serious buyers only.", "Bundles available.", ""]
TITLE_EXTRAS = ["with box", "clean IMEI", "no scratches", "screen protector on", "factory unlocked", "eSIM ready"]
REPOST_SUFFIXES = [(" - must go", 0.30), ("!!", 0.20), (" (relist)", 0.20), ("", 0.30)]


def seller_weights(n: int = N_SELLERS, s: float = SELLER_ZIPF_S) -> np.ndarray:
    ranks = np.arange(1, n + 1, dtype=np.float64)
    w = ranks ** (-s)
    return w / w.sum()


def canonical_catalog():
    """Build the canonical product catalog.

    Returns dict with parallel arrays: ids, bases (title base strings),
    price multipliers, sub_category, category_full, category, brand, and
    sampling weights (filled in later from calibration brand/leaf weights).
    """
    ids, bases, mults, subcats, catfulls, cats, brands = [], [], [], [], [], [], []

    for brand, models in PHONE_MODELS.items():
        for model, storages in models:
            for storage in storages:
                for color in COLORS:
                    ids.append(f"P:{brand}|{model}|{storage}|{color}")
                    bases.append(f"{brand} {model} {storage} {color}")
                    mults.append(STORAGE_MULT.get(storage, 1.0))
                    subcats.append("Cell Phones & Smartphones")
                    catfulls.append("Electronics/Cell Phones & Accessories/Cell Phones & Smartphones")
                    cats.append("Electronics")
                    brands.append(brand)

    for leaf, types in FURNITURE_TYPES.items():
        for typ, tmult in types:
            for material, mmult in FURNITURE_MATERIALS:
                for size, smult in FURNITURE_SIZES:
                    ids.append(f"F:{leaf}|{typ}|{material}|{size}")
                    bases.append(f"{typ} {material} {size}")
                    mults.append(tmult * mmult * smult)
                    subcats.append(leaf)
                    catfulls.append(f"Home/Furniture/{leaf}")
                    cats.append("Home")
                    brands.append(None)  # furniture brand drawn separately

    return {"ids": np.array(ids), "bases": np.array(bases), "mults": np.array(mults),
            "subcats": np.array(subcats), "catfulls": np.array(catfulls),
            "cats": np.array(cats), "brands": np.array(brands, dtype=object)}


def _variant_count(canonical: str) -> int:
    """Deterministic 6-15 title-variant budget per canonical product."""
    return 6 + zlib.crc32(canonical.encode("utf-8")) % 10


def _format_price_raw(price: np.ndarray, fmt_idx: np.ndarray, null_mask: np.ndarray, rng) -> np.ndarray:
    """Mixed price formats; null rows become '' or 'negotiable'."""
    out = np.empty(price.shape, dtype=object)
    finite = ~null_mask
    p = price

    m0 = finite & (fmt_idx == 0)   # "$15,000"
    m1 = finite & (fmt_idx == 1)   # "15000"
    m2 = finite & (fmt_idx == 2)   # "$15.0k"
    m3 = finite & (fmt_idx == 3)   # "$15,000/-"
    out[m0] = pd.Series(p[m0]).map("${:,.0f}".format)
    out[m1] = np.char.mod("%.0f", p[m1])
    out[m2] = pd.Series(p[m2]).map("${:.1f}k".format)
    out[m3] = pd.Series(p[m3]).map("${:,.0f}/-".format)
    neg = null_mask & (rng.random(price.shape[0]) < 0.5)
    out[null_mask] = "negotiable"
    out[neg] = ""
    return out


def generate_batch(batch_idx: int, n_rows: int, pools: dict, calib: dict, rng_seed: int,
                   start_seq: int = 0):
    rng = np.random.default_rng(rng_seed)
    n = n_rows

    # --- canonical product draw (weights encode the 62/38 category split) ---
    cat_idx = rng.choice(len(pools["cat_ids"]), size=n, p=pools["pool_weight"])
    canon_id = pools["cat_ids"][cat_idx]
    base = pools["cat_bases"][cat_idx]
    cat_mult = pools["cat_mults"][cat_idx]
    category = pools["cat_cats"][cat_idx]
    subcat = pools["cat_subcats"][cat_idx]
    catfull = pools["cat_catfulls"][cat_idx]
    brand = pools["cat_brands"][cat_idx]

    condition = (rng.choice(5, size=n, p=pools["cond_dist"]) + 1).astype(np.int64)

    # --- dates: 24-month horizon ---
    end = np.datetime64(datetime.now(timezone.utc).date())
    offsets = rng.integers(0, HORIZON_DAYS, size=n)
    posted64 = end - offsets.astype("timedelta64[D]")
    tts = rng.lognormal(np.log(TTS_MEDIAN_DAYS), TTS_SIGMA, size=n).astype(np.int64)
    delisted64 = posted64 + tts.astype("timedelta64[D]")
    sold = rng.random(n) >= ACTIVE_RATE
    missing_sale = rng.random(n) < SOLD_MISSING_RATE
    active = (~sold) | missing_sale | (delisted64 > end)

    age_months = (offsets / 30.44).astype(float)

    # --- prices ---
    cond_mult = pools["cond_mults"][condition - 1]
    e_mult = float(np.dot(pools["cond_dist"], pools["cond_mults"]))
    anchor = np.where(pools["cat_isphone"][cat_idx],
                      pd.Series(brand).map(pools["brand_anchors"]).fillna(80.0).to_numpy() / e_mult,
                      pools["furn_anchor"] / e_mult)
    price = anchor * cond_mult * cat_mult * (1.0 - AGE_DECLINE_PER_MONTH * age_months)
    price = np.maximum(price, PRICE_FLOOR)
    price = price * rng.lognormal(0.0, 0.18, size=n)

    # --- locations ---
    mk = rng.choice(len(MARKETS), size=n, p=[m[3] for m in MARKETS])
    cities = np.array([MARKETS[i][0] for i in mk], dtype=object)
    regions = np.array([MARKETS[i][1] for i in mk], dtype=object)
    aliases = np.array([MARKETS[i][2] for i in mk], dtype=object)
    city_mult = np.array([MARKETS[i][4] for i in mk])
    use_alias = np.array([a is not None for a in aliases]) & (rng.random(n) < ALIAS_SHARE)
    city = np.where(use_alias, aliases, cities)
    price = price * city_mult

    # --- outliers (~1%) ---
    outlier = rng.random(n) < OUTLIER_RATE
    price = np.where(outlier, price * rng.choice([0.1, 8.0], size=n), price)
    price = np.round(np.maximum(price, 1.0), 2)

    # --- null prices (blank / negotiable) ---
    price_null = rng.random(n) < NULL_PRICE
    price_f = np.where(price_null, np.nan, price)

    # --- titles: 12-template pool, deterministic 6-15 budget per canonical ---
    cond_word = np.take(np.array(COND_WORDS), condition - 1)
    nv = np.array([_variant_count(c) for c in canon_id])
    variant_idx = (rng.integers(0, 16, size=n)) % nv
    extra = rng.choice(TITLE_EXTRAS, size=n)

    B = pd.Series(base)
    CW = pd.Series(cond_word)
    CT = pd.Series(city).astype(str)
    EX = pd.Series(extra)

    t = B
    templates = [
        t,
        t + " " + CW,
        B.str.split(" ").str[-1] + " " + t,   # last-token-first (material/color)
        t + " (" + CW + ")",
        "Used " + t,
        t + " - Unlocked",
        t + " - " + CT + " pickup",
        t + " " + EX,
        "FS: " + t,
        t + " " + CW + " " + EX,
        "Selling " + t,
        t + " - " + CW,
    ]
    title = np.select([variant_idx == k for k in range(12)],
                      [tpl.to_numpy() for tpl in templates], default=t.to_numpy())

    # --- descriptions (5% null) ---
    cond_sent = np.take(np.array(COND_SENTENCES), condition - 1)
    opener = pd.Series(np.where(rng.random(n) < 0.5, "Selling my ", "For sale: "))
    extras_desc = pd.Series(rng.choice(DESC_EXTRAS, size=n, p=[0.15, 0.15, 0.15, 0.15, 0.15, 0.25]))
    desc = (opener + t + ". " + pd.Series(cond_sent) + " Pickup in " + CT + ". " + extras_desc).to_numpy()
    desc_null = rng.random(n) < NULL_DESC
    desc = np.where(desc_null, None, desc)

    # --- price_raw ---
    fmt_idx = rng.choice(4, size=n, p=[0.35, 0.30, 0.20, 0.15])
    price_raw = _format_price_raw(price_f, fmt_idx, price_null, rng)

    # --- seller / shipping ---
    seller = rng.choice([s for s, _ in SELLER_TYPES], size=n, p=[w for _, w in SELLER_TYPES])
    shipping = rng.choice([0, 1], size=n, p=[0.55, 0.45]).astype(np.int64)
    seller_num = rng.choice(N_SELLERS, size=n, p=pools["seller_w"])
    seller_id = np.array([f"S-{v:05d}" for v in seller_num])

    df = pd.DataFrame({
        "listing_id": [f"GEN-TMP-{start_seq + i:07d}" for i in range(n)],
        "title": title,
        "description": desc,
        "price": price_f,
        "price_raw": price_raw,
        "currency": "USD",
        "category": category,
        "sub_category": np.where(rng.random(n) < NULL_SUBCAT, None, subcat),
        "category_full": catfull,
        "item_condition_id": condition,
        "brand_name": brand,
        "shipping": shipping,
        "seller_id": seller_id,
        "location_city": city,
        "location_region": regions,
        "posted_date": posted64.astype(str),
        "delisted_date": np.where(active, None, delisted64.astype(str)),
        "seller_type": seller,
        "source_platform": "generated",
    })

    # ground truth for the originals ("_orig_pos" is resolved to a real id at finalize)
    truth = pd.DataFrame({
        "listing_id": df["listing_id"],
        "true_canonical_id": canon_id,
        "is_repost": np.zeros(n, dtype=bool),
        "original_listing_id": np.array([None] * n, dtype=object),
        "_orig_pos": np.array([np.nan] * n),
    })

    # --- reposts: same seller, near-identical title, posted 1-14 days later ---
    # reposts REPLACE trailing originals so every batch file stays exactly n rows;
    # originals are sampled only from the KEPT rows so links never dangle
    n_rep = int(round(n * REPOST_RATE))
    if n_rep > 0:
        keep = n - n_rep
        posted_dt = pd.to_datetime(df["posted_date"])
        end_ts = posted_dt.max()
        aged = ((end_ts - posted_dt).dt.days >= 14).to_numpy()
        eligible = np.nonzero(aged[:keep])[0]
        if len(eligible) == 0:
            n_rep = 0
    if n_rep > 0:
        src = rng.choice(eligible, size=min(n_rep, len(eligible)), replace=False)
        n_rep = len(src)
        rep = df.iloc[src].copy()

        # near-identical title: original title + small suffix (or identical)
        suf = np.array([s for s, _ in REPOST_SUFFIXES])[
            rng.choice(len(REPOST_SUFFIXES), size=n_rep, p=[w for _, w in REPOST_SUFFIXES])]
        rep["title"] = np.array([a + b for a, b in zip(rep["title"], suf)], dtype=object)

        # 1-14 days after the original (never past the horizon)
        shift = pd.to_timedelta(rng.integers(1, 15, size=n_rep), unit="D")
        max_shift = (end_ts - posted_dt.iloc[src]).dt.days.to_numpy()
        shift = pd.to_timedelta(np.minimum(rng.integers(1, 15, size=n_rep), np.maximum(max_shift, 1)), unit="D")
        rep["posted_date"] = (posted_dt.iloc[src] + shift).dt.strftime("%Y-%m-%d")
        dl = pd.to_datetime(rep["delisted_date"])
        new_dl = dl + shift
        rep["delisted_date"] = np.where(dl.notna() & (new_dl <= end_ts),
                                        new_dl.dt.strftime("%Y-%m-%d"), None)

        # seller is unchanged by design; price perturbed slightly; price_raw rebuilt
        rep["price"] = (rep["price"] * rng.uniform(0.95, 1.05, size=n_rep)).round(2)
        rep_null = rep["price"].isna().to_numpy()
        rep["price_raw"] = _format_price_raw(rep["price"].to_numpy(),
                                             rng.choice(4, size=n_rep, p=[0.35, 0.30, 0.20, 0.15]),
                                             rep_null, rng)

        rep_truth = pd.DataFrame({
            "listing_id": rep["listing_id"],
            "true_canonical_id": canon_id[src],
            "is_repost": np.ones(n_rep, dtype=bool),
            "original_listing_id": np.array([None] * n_rep, dtype=object),
            "_orig_pos": src.astype(float),
        })

        df = pd.concat([df.iloc[:keep], rep], ignore_index=True)
        truth = pd.concat([truth.iloc[:keep], rep_truth], ignore_index=True)

    return df, truth


def finalize_ids(df: pd.DataFrame, truth: pd.DataFrame, batch_idx: int, start_seq: int):
    df = df.copy()
    truth = truth.copy()
    ids = np.array([f"GEN-{batch_idx:03d}-{start_seq + i:06d}" for i in range(len(df))])
    df["listing_id"] = ids
    truth["listing_id"] = ids
    # resolve repost -> original links via stored positions (all < len(df))
    pos = truth["_orig_pos"].to_numpy()
    known = ~np.isnan(pos)
    resolved = np.array([None] * len(truth), dtype=object)
    resolved[known] = ids[pos[known].astype(int)]
    truth["original_listing_id"] = resolved
    truth = truth.drop(columns=["_orig_pos"])
    return df, truth


def main() -> None:
    ap = argparse.ArgumentParser(description="ResellRadar synthetic listing generator")
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--chunksize", type=int, default=CHUNK_SIZE)
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--outdir", default=DEFAULT_OUT)
    ap.add_argument("--truthdir", default=DEFAULT_TRUTH)
    ap.add_argument("--run-dir", default=None, help="resume into an existing run= partition")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    import psutil
    proc = psutil.Process()
    t0 = datetime.now()

    calib = load_calibration()
    catalog = canonical_catalog()
    n_canon = len(catalog["ids"])
    print(f"[catalog] canonical products: {n_canon} "
          f"(phones {int((catalog['cats'] == 'Electronics').sum())}, "
          f"furniture {int((catalog['cats'] == 'Home').sum())})")

    # pool weights: brand/leaf weights from calibration spread over their canonicals
    brand_w = {b: w for b, w, _m in calib["phone_brands"]}
    brand_anchor = {b: m for b, _w, m in calib["phone_brands"]}
    # catalog brands too rare in the real seed sample (<30 rows) get a small
    # synthetic share (split by the calibration fallback weights) so that every
    # catalog canonical can actually be realized in the data
    missing = sorted({b for b in catalog["brands"] if b and b not in brand_w})
    if missing:
        fb = {b: (w, a) for b, w, a in FALLBACK_PHONE_BRANDS}
        fb_tot = sum(fb.get(b, (0.01, 0.0))[0] for b in missing)
        for b in missing:
            w, a = fb.get(b, (0.01, 80.0))
            brand_w[b] = FALLBACK_BRAND_SHARE * w / fb_tot
            brand_anchor[b] = a
        print(f"[catalog] {len(missing)} catalog brands below seed-sample threshold -> "
              f"synthetic share {FALLBACK_BRAND_SHARE:.1%}: {', '.join(missing)}")
    leaf_w = dict(calib["furniture_leaves"])
    raw_w = []
    for cid, cat, brand in zip(catalog["ids"], catalog["cats"], catalog["brands"]):
        if cat == "Electronics":
            raw_w.append(brand_w.get(brand, 1e-6))
        else:
            leaf = cid.split("|")[0][2:]
            raw_w.append(leaf_w.get(leaf, 1e-6))
    pool_w = np.asarray(raw_w, dtype=np.float64)
    phone_mask = catalog["cats"] == "Electronics"
    pool_w[phone_mask] *= 0.62 / pool_w[phone_mask].sum()
    pool_w[~phone_mask] *= 0.38 / pool_w[~phone_mask].sum()

    pools = {
        "cat_ids": catalog["ids"], "cat_bases": catalog["bases"], "cat_mults": catalog["mults"],
        "cat_subcats": catalog["subcats"], "cat_catfulls": catalog["catfulls"],
        "cat_cats": catalog["cats"], "cat_brands": catalog["brands"],
        "cat_isphone": phone_mask,
        "pool_weight": pool_w,
        "brand_anchors": brand_anchor,
        "cond_mults": np.array(calib["condition_mults"]),
        "cond_dist": np.array(calib["condition_dist"]),
        "furn_anchor": calib["furniture_price_anchor"],
        "seller_w": seller_weights(),
    }
    print(f"[catalog] sampling weights ready (62/38 phone-furniture split); "
          f"top seller share {pools['seller_w'][0]:.2%}")

    if args.run_dir:
        run_dir = os.path.join(PROJECT_ROOT, args.run_dir) if not os.path.isabs(args.run_dir) else args.run_dir
    else:
        run_name = "run=" + datetime.now(timezone.utc).strftime("%Y_%m_%dT%H%M%SZ")
        run_dir = os.path.join(args.outdir, run_name)
        truth_dir = os.path.join(args.truthdir, run_name)
    run_name = os.path.basename(run_dir)
    truth_dir = os.path.join(args.truthdir, run_name)
    os.makedirs(run_dir, exist_ok=True)
    os.makedirs(truth_dir, exist_ok=True)

    existing = sorted(f for f in os.listdir(run_dir) if f.endswith(".jsonl"))
    if existing and not args.resume:
        raise SystemExit(f"[error] {run_dir} already contains {len(existing)} batch files. "
                         f"Use --resume to continue, or omit --run-dir for a fresh run.")
    start_batch = len(existing) + 1
    if existing:
        print(f"[resume] {len(existing)} batches exist; continuing at batch {start_batch:03d}")

    total_batches = (args.rows + args.chunksize - 1) // args.chunksize
    print(f"[plan] rows={args.rows:,} chunk={args.chunksize:,} batches={total_batches} "
          f"-> {os.path.relpath(run_dir, PROJECT_ROOT)}")

    today = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    peak_rss = 0
    rows_done = 0
    seq = 0
    truth_frames = []
    for b in range(start_batch, total_batches + 1):
        n = min(args.chunksize, args.rows - (b - 1) * args.chunksize)
        df, truth = generate_batch(b, n, pools, calib, rng_seed=args.seed + b, start_seq=seq)
        df, truth = finalize_ids(df, truth, b, seq)
        fname = f"gen_{today}_batch{b:03d}.jsonl"
        fpath = os.path.join(run_dir, fname)
        if os.path.exists(fpath):
            raise SystemExit(f"[error] refusing to overwrite existing raw file: {fname} (raw zone is immutable)")
        df.to_json(fpath, orient="records", lines=True, force_ascii=False)
        truth_frames.append(truth)
        rows_done += len(df)
        seq += len(df)
        rss = proc.memory_info().rss / 1e6
        peak_rss = max(peak_rss, rss)
        if b == start_batch or b % 10 == 0 or b == total_batches:
            print(f"[batch {b:03d}/{total_batches}] {len(df):,} rows -> {fname} "
                  f"({os.path.getsize(fpath):,} bytes, rss {rss:.0f} MB)")

    truth_all = pd.concat(truth_frames, ignore_index=True)
    truth_path = os.path.join(truth_dir, "truth.parquet")
    truth_all.to_parquet(truth_path, index=False)
    n_rep_total = int(truth_all["is_repost"].sum())

    dt = (datetime.now() - t0).total_seconds()
    files = sorted(f for f in os.listdir(run_dir) if f.endswith(".jsonl"))
    total_bytes = sum(os.path.getsize(os.path.join(run_dir, f)) for f in files)
    print("=== generation summary ===")
    print(f"rows written : {rows_done:,} (raw) | truth: {len(truth_all):,} rows "
          f"({n_rep_total:,} reposts, {n_rep_total / len(truth_all):.1%})")
    print(f"files        : {len(files)} in {os.path.relpath(run_dir, PROJECT_ROOT)}")
    print(f"ground truth : {os.path.relpath(truth_path, PROJECT_ROOT)} "
          f"({os.path.getsize(truth_path):,} bytes)")
    print(f"bytes        : {total_bytes:,}")
    print(f"runtime      : {dt:.1f}s ({rows_done / max(dt, 1e-9):,.0f} rows/s)")
    print(f"peak rss     : {peak_rss:.0f} MB")
    print(f"seed         : {args.seed} (per-batch rng: seed + batch index)")


if __name__ == "__main__":
    main()
