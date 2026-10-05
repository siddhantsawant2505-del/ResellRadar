"""
ResellRadar - manual data ingestion: normalize + validate an uploaded file into
the raw zone so it joins the next pipeline run (Person 1, upload integration).

The pipeline union lives in spark_jobs/clean_normalize.py: it reads
    data/raw/mercari/   (real source, source_platform="mercari")
    data/raw/generated/ (synthetic, source_platform="generated")
This script adds a third zone:
    data/raw/uploads/run=<ts>/uploads.jsonl  (source_platform="uploads")

Every uploaded row is normalized to the 19-field union schema, validated
(title + price required, condition 1-5, shipping 0/1), stamped with
source_platform="uploads", and written as JSONL. Rows whose listing_id already
exists in the raw zone are dropped (first write wins) so re-uploads and
collisions with the existing corpus can never duplicate listings - the Spark
stages recompute the whole zone, so merge-with-processed == full recompute
including uploads.

Usage:
    python scripts/ingest_uploads.py --file <path> [--source-name mybatch]
Python API:
    ingest_file(path, source_name=None) -> dict summary
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_ROOT = os.path.join(PROJECT_ROOT, "data", "raw")

# Canonical union schema (docs/SCHEMA_mercari.md, docs/SCHEMA_generated.md).
UNION_FIELDS = [
    "listing_id", "title", "description", "price", "price_raw", "currency",
    "category", "sub_category", "category_full", "item_condition_id",
    "brand_name", "shipping", "posted_date", "delisted_date", "location_city",
    "location_region", "seller_type", "seller_id", "source_platform",
]

# Header aliases -> canonical field (mirrors the mappings in clean_normalize.py).
HEADER_ALIASES = {
    "train_id": "listing_id",
    "id": "listing_id",
    "name": "title",
    "item_description": "description",
    "category_name": "category_full",
    "item_condition": "item_condition_id",
    "condition": "item_condition_id",
    "brand": "brand_name",
    "merchant_rating": "seller_type",
}

MAX_UPLOAD_BYTES = 200 * 1024 * 1024  # 200 MB per file


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", str(h).strip().lower().replace(" ", "_"))


def _to_float(v):
    if v is None:
        return None
    s = str(v).replace("$", "").replace(",", "").strip()
    if not s or s.lower() in {"n/a", "na", "null", "none", "negotiable"}:
        return None
    try:
        f = float(s)
        return f if f > 0 else None
    except ValueError:
        return None


def _to_int(v, lo=None, hi=None):
    try:
        f = float(str(v).strip())
        i = int(f)
        if f != i:  # "3.5" is not a condition id
            return None
    except (TypeError, ValueError):
        return None
    if lo is not None and i < lo:
        return None
    if hi is not None and i > hi:
        return None
    return i


def _to_ts(v):
    if v is None or str(v).strip() == "":
        return None
    s = str(v).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None  # unparseable dates become NULL (never reject the row for them)


def _existing_listing_ids() -> set:
    """listing_ids already in the raw zone, so uploads can never duplicate them."""
    import pandas as pd  # local import: only needed when a raw zone exists

    ids = set()
    for zone in ("mercari", "generated"):
        zone_dir = os.path.join(RAW_ROOT, zone)
        if not os.path.isdir(zone_dir):
            continue
        for root, _dirs, names in os.walk(zone_dir):
            for fn in names:
                p = os.path.join(root, fn)
                try:
                    if fn.endswith(".tsv"):
                        df = pd.read_csv(p, sep="\t", usecols=["train_id"])
                        ids.update(str(v) for v in df["train_id"].dropna())
                    elif fn.endswith(".csv"):
                        df = pd.read_csv(p, usecols=lambda c: _norm_header(c) in ("listing_id", "train_id"))
                        col = df.columns[0]
                        ids.update(str(v) for v in df[col].dropna())
                    elif fn.endswith(".jsonl"):
                        with open(p, encoding="utf-8") as fh:
                            for line in fh:
                                if line.strip():
                                    rec = json.loads(line)
                                    if rec.get("listing_id") is not None:
                                        ids.add(str(rec["listing_id"]))
                    elif fn.endswith(".json"):
                        with open(p, encoding="utf-8") as fh:
                            data = json.load(fh)
                        if isinstance(data, list):
                            ids.update(str(r["listing_id"]) for r in data if isinstance(r, dict) and r.get("listing_id") is not None)
                except Exception:
                    continue  # a malformed legacy file must not block ingestion
    return ids


def _read_rows(path: str):
    """Yield (row_dict, error_str) from CSV / TSV / JSON / JSONL."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".json", ".jsonl"):
        opener = open(path, encoding="utf-8")
        try:
            if ext == ".jsonl":
                for ln, line in enumerate(opener, 1):
                    if not line.strip():
                        continue
                    try:
                        yield json.loads(line), None
                    except json.JSONDecodeError as e:
                        yield None, f"line {ln}: invalid JSON ({e})"
            else:
                data = json.load(opener)
                if isinstance(data, dict):  # {"items": [...]} style wrappers
                    data = next((v for v in data.values() if isinstance(v, list)), None)
                if not isinstance(data, list):
                    yield None, "JSON root must be an array of objects"
                    return
                for i, rec in enumerate(data):
                    if isinstance(rec, dict):
                        yield rec, None
                    else:
                        yield None, f"item {i}: not an object"
        finally:
            opener.close()
        return

    sep = "\t" if ext == ".tsv" else ","
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            yield None, "no header row found"
            return
        mapping = {}
        for raw_h in reader.fieldnames:
            h = _norm_header(raw_h)
            mapping[raw_h] = HEADER_ALIASES.get(h, h)
        for ln, row in enumerate(reader, 2):
            yield {mapping[k]: v for k, v in row.items() if k is not None}, None


def _normalize(rec: dict, source_name: str):
    """Return (clean_row, error). Missing description etc. become NULL."""
    title = str(rec.get("title") or "").strip()
    if not title:
        return None, "missing title"
    price = _to_float(rec.get("price"))
    if price is None:
        return None, "missing or invalid price"
    if rec.get("listing_id") is not None and str(rec.get("listing_id")).strip() != "":
        listing_id = str(rec.get("listing_id")).strip()
    else:
        listing_id = f"up_{source_name}_{abs(hash((title, price, str(rec.get('seller_id'))))) % 10**10}"

    row = {f: None for f in UNION_FIELDS}
    row.update({
        "listing_id": listing_id,
        "title": title[:300],
        "description": (str(rec.get("description")).strip() or None) if rec.get("description") is not None else None,
        "price": round(price, 2),
        "price_raw": str(rec.get("price_raw") or "").strip() or str(rec.get("price")),
        "currency": str(rec.get("currency") or "USD").strip().upper()[:8],
        "category": (str(rec.get("category")).strip() or None) if rec.get("category") is not None else None,
        "sub_category": (str(rec.get("sub_category")).strip() or None) if rec.get("sub_category") is not None else None,
        "category_full": (str(rec.get("category_full")).strip() or None) if rec.get("category_full") is not None else None,
        "item_condition_id": _to_int(rec.get("item_condition_id"), 1, 5),
        "brand_name": (str(rec.get("brand_name")).strip() or None) if rec.get("brand_name") is not None else None,
        "shipping": _to_int(rec.get("shipping"), 0, 1),
        "posted_date": _to_ts(rec.get("posted_date")),
        "delisted_date": _to_ts(rec.get("delisted_date")),
        "location_city": (str(rec.get("location_city")).strip() or None) if rec.get("location_city") is not None else None,
        "location_region": (str(rec.get("location_region")).strip() or None) if rec.get("location_region") is not None else None,
        "seller_type": (str(rec.get("seller_type")).strip() or None) if rec.get("seller_type") is not None else None,
        "seller_id": (str(rec.get("seller_id")).strip() or None) if rec.get("seller_id") is not None else None,
        "source_platform": "uploads",
    })
    return row, None


def ingest_file(path: str, source_name: str = None) -> dict:
    """Normalize + validate + store an upload. Returns a summary dict."""
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"upload not found: {path}")
    size = os.path.getsize(path)
    if size > MAX_UPLOAD_BYTES:
        raise ValueError(f"file too large ({size:,} bytes; limit {MAX_UPLOAD_BYTES:,})")
    if os.path.splitext(path)[1].lower() not in (".csv", ".tsv", ".json", ".jsonl"):
        raise ValueError("unsupported format: use CSV, TSV, JSON, or JSONL")

    safe_name = re.sub(r"[^A-Za-z0-9_-]", "", (source_name or os.path.splitext(os.path.basename(path))[0]).lower())[:40] or "upload"
    ts = datetime.now(timezone.utc).strftime("%Y_%m_%dT%H%M%SZ")
    run_dir = os.path.join(RAW_ROOT, "uploads", f"run={ts}")
    os.makedirs(run_dir, exist_ok=True)
    out_path = os.path.join(run_dir, f"{safe_name}.jsonl")

    seen_in_batch = set()
    existing = _existing_listing_ids()
    accepted, rejected, duplicate_existing = 0, 0, 0
    reject_samples = []

    with open(out_path, "w", encoding="utf-8") as out:
        for rec, err in _read_rows(path):
            if err:
                rejected += 1
                if len(reject_samples) < 5:
                    reject_samples.append(err)
                continue
            row, verr = _normalize(rec, safe_name)
            if verr:
                rejected += 1
                if len(reject_samples) < 5:
                    reject_samples.append(f"{verr}: {str(rec)[:80]}")
                continue
            lid = row["listing_id"]
            if lid in existing or lid in seen_in_batch:
                duplicate_existing += 1
                continue
            seen_in_batch.add(lid)
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            accepted += 1

    if accepted == 0:
        os.remove(out_path)
        os.rmdir(run_dir)
        raise ValueError(
            f"no valid rows accepted ({rejected} rejected, {duplicate_existing} duplicates of existing listings)"
            + (f"; first issues: {reject_samples}" if reject_samples else "")
        )

    summary = {
        "accepted": accepted,
        "rejected": rejected,
        "duplicates_of_existing": duplicate_existing,
        "reject_samples": reject_samples,
        "source_file": os.path.basename(path),
        "source_name": safe_name,
        "raw_path": os.path.relpath(out_path, PROJECT_ROOT).replace(os.sep, "/"),
        "bytes": os.path.getsize(out_path),
        "stored_at": _now(),
    }
    with open(os.path.join(run_dir, "ingest_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"[ingest] accepted={accepted:,} rejected={rejected:,} "
          f"duplicates={duplicate_existing:,} -> {summary['raw_path']}")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Normalize + validate a manual upload into the raw zone.")
    ap.add_argument("--file", required=True, help="CSV / TSV / JSON / JSONL file to ingest")
    ap.add_argument("--source-name", default=None, help="short label embedded in listing ids")
    args = ap.parse_args()
    try:
        ingest_file(args.file, args.source_name)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
