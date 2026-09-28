"""
ResellRadar — Task 1: Mercari download/extraction (Person 1).

Ensures data/raw/mercari/train.tsv exists, verifies it, and records SHA-256
checksums in logs/checksums.txt. The file is REAL data and is never modified.

Strategy (in order):
  1. If data/raw/mercari/train.tsv already exists  -> verify + checksum, done.
  2. If an archive (train.tsv.7z / train.tsv.zip / train.tsv) exists locally
     (e.g. data/train.tsv.7z)                      -> extract it.
  3. Else try the Kaggle CLI:
       kaggle competitions download -c mercari-price-suggestion-challenge -f train.tsv
     Credentials are read from ~/.kaggle/kaggle.json; if missing or if the
     competition rules were not accepted, we print exact instructions and exit
     non-zero (no silent fallback to unverified third-party mirrors).

Usage:
    python scripts/download_data.py

Exit code 0 = train.tsv present and verified.
"""

import hashlib
import os
import sys
import zipfile
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_MERCARI = os.path.join(PROJECT_ROOT, "data", "raw", "mercari")
TARGET_TSV = os.path.join(RAW_MERCARI, "train.tsv")
CHECKSUMS = os.path.join(PROJECT_ROOT, "logs", "checksums.txt")

COMPETITION = "mercari-price-suggestion-challenge"
EXPECTED_HEADER = "train_id\tname\titem_condition_id\tcategory_name\tbrand_name\tprice\tshipping\titem_description"

HELP_TEXT = """
Kaggle credentials missing OR competition rules not accepted.

1.  Create/verify a Kaggle account (free).
2.  Accept the competition rules ON THE WEBSITE:
        https://www.kaggle.com/competitions/mercari-price-suggestion-challenge/rules
    (the CLI will 403 until you click "I Understand and Accept").
3.  Get an API token: kaggle.com -> Settings -> API -> "Create New Token".
    This downloads kaggle.json.
4.  Place it at ~/.kaggle/kaggle.json
        Windows:  C:\\Users\\<you>\\.kaggle\\kaggle.json
        Linux/macOS: ~/.kaggle/kaggle.json
5.  pip install kaggle
6.  Re-run:  python scripts/download_data.py

Alternatively, download train.tsv (or its .7z archive) manually from the
competition "Data" tab and place it at  data/train.tsv.7z  (or data/train.tsv),
then re-run this script. It will detect and extract it.
"""


def sha256_file(path: str, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb", chunk_size) as f:
        for block in iter(lambda: f.read(chunk_size), b""):
            h.update(block)
    return h.hexdigest()


def append_checksum(path: str) -> None:
    os.makedirs(os.path.dirname(CHECKSUMS), exist_ok=True)
    digest = sha256_file(path)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    size = os.path.getsize(path)
    rel = os.path.relpath(path, PROJECT_ROOT)
    with open(CHECKSUMS, "a", encoding="utf-8") as f:
        f.write(f"{digest}  sha256  {size:>14,d} bytes  {ts}  {rel}\n")
    print(f"[checksum] {digest[:16]}...  {rel}  ({size:,} bytes) -> {os.path.relpath(CHECKSUMS, PROJECT_ROOT)}")


def find_local_source() -> str | None:
    """Prefer an already-extracted TSV anywhere, else an archive. Nearest to the target wins."""
    candidates = [
        TARGET_TSV,
        os.path.join(ARCHIVE_DIR, "train.tsv"),
        os.path.join(ARCHIVE_DIR, "train.tsv.7z"),
        os.path.join(ARCHIVE_DIR, "train.tsv.zip"),
        os.path.join(RAW_MERCARI, "train.tsv.7z"),
        os.path.join(RAW_MERCARI, "train.tsv.zip"),
    ]
    for c in candidates:
        if os.path.exists(c) and os.path.getsize(c) > 0:
            return c
    return None


def extract(source: str) -> None:
    os.makedirs(RAW_MERCARI, exist_ok=True)
    print(f"[extract] {os.path.relpath(source, PROJECT_ROOT)} -> {os.path.relpath(TARGET_TSV, PROJECT_ROOT)}")
    if source.endswith(".7z"):
        try:
            import py7zr
        except ImportError:
            sys.exit("[error] py7zr not installed. Run: pip install py7zr")
        with py7zr.SevenZipFile(source, mode="r") as z:
            names = z.getnames()
            target_name = next((n for n in names if os.path.basename(n) == "train.tsv"), None)
            if target_name is None:
                sys.exit(f"[error] train.tsv not found inside archive (contains: {names[:10]}...)")
            z.extract(targets=[target_name], path=RAW_MERCARI)
            # py7zr may recreate directories for nested names; normalize location.
            extracted = os.path.join(RAW_MERCARI, target_name)
            if extracted != TARGET_TSV:
                os.replace(extracted, TARGET_TSV)
    elif source.endswith(".zip"):
        with zipfile.ZipFile(source) as z:
            member = next((m for m in z.namelist() if os.path.basename(m) == "train.tsv"), None)
            if member is None:
                sys.exit(f"[error] train.tsv not found inside archive (contains: {z.namelist()[:10]}...)")
            with z.open(member) as src, open(TARGET_TSV, "wb") as dst:
                dst.write(src.read())
    else:  # plain tsv
        if os.path.abspath(source) != os.path.abspath(TARGET_TSV):
            os.replace(source, TARGET_TSV)


def verify_tsv() -> int:
    """Verify header; count rows via chunked reads. Returns data-row count."""
    import pandas as pd

    with open(TARGET_TSV, "r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n\r")
    if header != EXPECTED_HEADER:
        sys.exit(f"[error] Unexpected header.\n  expected: {EXPECTED_HEADER}\n  found:    {header}")

    rows = 0
    for chunk in pd.read_csv(TARGET_TSV, sep="\t", usecols=["train_id"], chunksize=1_000_000):
        rows += len(chunk)
    return rows


def main() -> None:
    print("=== ResellRadar Task 1: Mercari source (REAL data) ===")

    if os.path.exists(TARGET_TSV) and os.path.getsize(TARGET_TSV) > 0:
        print(f"[ok] train.tsv already extracted at {os.path.relpath(TARGET_TSV, PROJECT_ROOT)}")
    else:
        source = find_local_source()
        if source is None:
            print("[download] No local archive found; attempting Kaggle CLI download…")
            rc = os.system(f'kaggle competitions download -c {COMPETITION} -f train.tsv -p "{ARCHIVE_DIR}"')
            source = find_local_source()
            if rc != 0 or source is None:
                print(HELP_TEXT)
                sys.exit(1)
        extract(source)

    print("[verify] header + row count (chunked)…")
    rows = verify_tsv()
    expected = 1_482_535
    flag = "ok" if abs(rows - expected) / expected < 0.02 else "MISMATCH (investigate!)"
    print(f"[verify] rows={rows:,} (expected ~{expected:,}) -> {flag}")

    append_checksum(TARGET_TSV)
    print("[done] Raw Mercari source ready and immutable: data/raw/mercari/train.tsv")


if __name__ == "__main__":
    main()
