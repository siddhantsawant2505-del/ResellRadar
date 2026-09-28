"""
ResellRadar — Task 2: seed-sample extraction (Person 1).

One-time chunked pass over data/raw/mercari/train.tsv that extracts all
phone + furniture listings into data/seed/mercari_phone_furniture_sample.csv.
This sample is the calibration input for generator/generate_data.py: product
titles, brands, price levels, and Mercari-style category strings.

The raw TSV is only ever READ here — immutability is preserved.

Usage:
    python scripts/extract_seed_sample.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from download_data import PROJECT_ROOT, TARGET_TSV, append_checksum  # noqa: E402

SAMPLE_PATH = os.path.join(PROJECT_ROOT, "data", "seed", "mercari_phone_furniture_sample.csv")
SEED = 42
CAP = 250_000  # safety cap; expected matches are well below this
CHUNK_ROWS = 300_000

PHONE_PREFIX = "Electronics/Cell Phones & Accessories/"
PHONE_LEAF = PHONE_PREFIX + "Cell Phones & Smartphones"
FURNITURE_PREFIX = "Home/Furniture/"

USECOLS = ["train_id", "name", "item_condition_id", "category_name", "brand_name", "price", "shipping"]


def is_seed_row(cat) -> bool:
    if not isinstance(cat, str):
        return False
    return cat == PHONE_LEAF or cat.startswith(FURNITURE_PREFIX)


def main() -> None:
    import pandas as pd

    if not os.path.exists(TARGET_TSV):
        sys.exit("[error] train.tsv missing - run scripts/download_data.py first")

    if os.path.exists(SAMPLE_PATH):
        n = sum(1 for _ in open(SAMPLE_PATH, encoding="utf-8")) - 1
        print(f"[ok] seed sample already exists ({n:,} rows): {os.path.relpath(SAMPLE_PATH, PROJECT_ROOT)}")
        print("[ok] delete it to force re-extraction")
        return

    print("=== ResellRadar Task 2: Mercari phone/furniture seed sample ===")
    print(f"[scan] chunked pass over train.tsv ({CHUNK_ROWS:,} rows/chunk)…")

    matches = []
    scanned = 0
    for chunk in pd.read_csv(TARGET_TSV, sep="\t", usecols=USECOLS, chunksize=CHUNK_ROWS):
        scanned += len(chunk)
        mask = chunk["category_name"].map(is_seed_row)
        if mask.any():
            matches.append(chunk[mask])
        print(f"\r[scan] {scanned:,} rows, {sum(len(m) for m in matches):,} matches", end="", flush=True)
    print()

    df = pd.concat(matches, ignore_index=True)
    print(f"[scan] matched {len(df):,} phone/furniture listings out of {scanned:,}")

    if len(df) > CAP:
        df = df.sample(n=CAP, random_state=SEED).reset_index(drop=True)
        print(f"[sample] down-sampled to {CAP:,} (seed={SEED})")

    os.makedirs(os.path.dirname(SAMPLE_PATH), exist_ok=True)
    df.to_csv(SAMPLE_PATH, index=False)
    print(f"[write] {os.path.relpath(SAMPLE_PATH, PROJECT_ROOT)} ({os.path.getsize(SAMPLE_PATH):,} bytes)")

    leaf_counts = df["category_name"].str.split("/").str[-1].value_counts().head(12)
    print("[profile] leaf category breakdown:")
    for leaf, count in leaf_counts.items():
        print(f"    {leaf:<40} {count:>9,}")
    print(f"[profile] brand null rate: {df['brand_name'].isna().mean():.1%}")
    print(f"[profile] price: min={df['price'].min():.2f} median={df['price'].median():.2f} max={df['price'].max():.2f}")
    phones = df[df["category_name"].eq(PHONE_LEAF)]
    if len(phones):
        print(f"[profile] phones: n={len(phones):,}, brand null {phones['brand_name'].isna().mean():.0%}, "
              f"price median={phones['price'].median():.0f}, p90={phones['price'].quantile(0.9):.0f}, max={phones['price'].max():.0f}")
    else:
        print("[profile] phones: 0 rows in real data - generator will use furniture-informed "
              "catalog for both, with phone prices from published launch-price curves")

    append_checksum(SAMPLE_PATH)
    print("[done] seed sample ready for the generator")


if __name__ == "__main__":
    main()
