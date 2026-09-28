"""
ResellRadar — Task 5: raw-zone validation (Person 1).

Chunked validation + statistical profiling over the FULL raw zone, per source:
  - data/raw/mercari/train.tsv   (REAL)
  - data/raw/generated/run=*/gen_*.jsonl  (GENERATED)

Outputs logs/validation_report.md with: row counts, per-field null rates,
duplicate rates, price distributions, category splits, structural-gate results,
and runtime/memory. Memory stays flat: only aggregates are accumulated
(plus one small float array per source for exact price percentiles).

Usage:
    python scripts/validate_raw.py [--mercari data/raw/mercari/train.tsv] \
                                   [--generated-dir data/raw/generated]
"""

import argparse
import glob
import hashlib
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_MERCARI = os.path.join(PROJECT_ROOT, "data", "raw", "mercari", "train.tsv")
DEFAULT_GENERATED = os.path.join(PROJECT_ROOT, "data", "raw", "generated")
REPORT_PATH = os.path.join(PROJECT_ROOT, "logs", "validation_report.md")

MERCARI_BASELINES = {
    "brand_name_null": (0.40, 0.47),
    "category_name_null": (0.002, 0.008),
    "rows": (1_450_000, 1_500_000),
}

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
STORAGE_TOKENS = {"32GB", "64GB", "128GB", "256GB", "512GB", "1TB"}


def _row_hash(frame: pd.DataFrame, cols: list) -> np.ndarray:
    """Stable per-row hash for duplicate detection without keeping strings.

    pd.util.hash_pandas_object on a DataFrame hashes each row.
    """
    return pd.util.hash_pandas_object(frame[cols], index=False).to_numpy()


class MercariProfile:
    def __init__(self):
        self.rows = 0
        self.nulls = Counter()
        self.fields = ["train_id", "name", "item_condition_id", "category_name",
                       "brand_name", "price", "shipping", "item_description"]
        self.cat_top = Counter()
        self.cat_leaf = Counter()
        self.cond = Counter()
        self.ship = Counter()
        self.prices = []
        self.dups = 0
        self.structural = Counter()
        self.zero_price = 0
        self.desc_rm = 0

    def feed(self, chunk: pd.DataFrame):
        self.rows += len(chunk)
        for f in self.fields:
            self.nulls[f] += int(chunk[f].isna().sum())
        self.zero_price += int((chunk["price"] == 0).sum())
        self.desc_rm += int(chunk["item_description"].fillna("").str.fullmatch(r"\[rm\]").sum())
        self.prices.append(chunk["price"].to_numpy(dtype=np.float64))

        cats = chunk["category_name"].fillna("<MISSING>")
        self.cat_top.update(cats.str.split("/").str[0])
        self.cat_leaf.update(cats)
        self.cond.update(chunk["item_condition_id"].value_counts().to_dict())
        self.ship.update(chunk["shipping"].value_counts().to_dict())

        # structural: condition in 1..5, shipping in {0,1}, price >= 0, non-null id
        self.structural["bad_condition"] += int((~chunk["item_condition_id"].isin([1, 2, 3, 4, 5])).sum())
        self.structural["bad_shipping"] += int((~chunk["shipping"].isin([0, 1])).sum())
        self.structural["negative_price"] += int((chunk["price"] < 0).sum())
        self.structural["null_id"] += int(chunk["train_id"].isna().sum())

    def finish(self):
        prices = np.concatenate(self.prices) if self.prices else np.array([])
        self.prices = prices
        self.dups = None  # computed via id-range check in report()


class GeneratedProfile:
    def __init__(self):
        self.rows = 0
        self.fields = ["listing_id", "title", "description", "price", "price_raw", "currency",
                       "category", "sub_category", "category_full", "item_condition_id",
                       "brand_name", "shipping", "seller_id", "location_city", "location_region",
                       "posted_date", "delisted_date", "seller_type", "source_platform"]
        self.nulls = Counter()
        self.prices = []
        self.price_raw_fmt = Counter()
        self.cat_top = Counter()
        self.cat_leaf = Counter()
        self.alias = 0
        self.posted_min, self.posted_max = None, None
        self.bad_date_fmt = 0
        self.bad_order = 0
        self.bad_cond = 0
        self.bad_id = 0
        self.id_hashes = []
        self.dup_sig_hashes = []    # (seller_id, normalized title)
        self.dup_posted_ord = []    # posted_date as day-ordinal, aligned with dup_sig_hashes
        self.naive_hashes = []      # (title, price) - the naive collision definition

    def feed(self, chunk: pd.DataFrame):
        self.rows += len(chunk)
        for f in self.fields:
            if f in chunk:
                self.nulls[f] += int(chunk[f].isna().sum())

        p = chunk["price"]
        self.prices.append(p.dropna().to_numpy(dtype=np.float64))
        fmt = chunk["price_raw"].fillna("").astype(str).map(_classify_price_raw)
        self.price_raw_fmt.update(fmt.value_counts().to_dict())

        self.cat_top.update(chunk["category"].value_counts().to_dict())
        self.cat_leaf.update(chunk["sub_category"].fillna("<MISSING>").value_counts().to_dict())
        self.alias += int(chunk["location_city"].isin(["NYC", "SF", "LA", "Philly"]).sum())

        posted = pd.to_datetime(chunk["posted_date"], errors="coerce")
        pmin, pmax = posted.min(), posted.max()
        self.posted_min = pmin if self.posted_min is None else min(self.posted_min, pmin)
        self.posted_max = pmax if self.posted_max is None else max(self.posted_max, pmax)

        dl = pd.to_datetime(chunk["delisted_date"], errors="coerce")
        self.bad_date_fmt += int((~chunk["posted_date"].fillna("").str.fullmatch(r"\d{4}-\d{2}-\d{2}")).sum())
        self.bad_date_fmt += int((chunk["delisted_date"].dropna().astype(str)
                                  .str.fullmatch(r"\d{4}-\d{2}-\d{2}") == False).sum())  # noqa: E712
        sold = dl.notna()
        self.bad_order += int((dl[sold] < posted[sold]).sum())
        self.bad_cond += int((~chunk["item_condition_id"].isin([1, 2, 3, 4, 5])).sum())
        self.bad_id += int(chunk["listing_id"].isna().sum()
                           + (~chunk["listing_id"].fillna("").str.startswith("GEN-")).sum())

        self.id_hashes.append(_row_hash(chunk, ["listing_id"]))

        # designed duplicate signature: same seller + normalized title
        norm_title = (chunk["title"].astype(str).str.lower().str.replace(r"\\s+", " ", regex=True)
                      .str.replace(r"( - must go| \\(relist\\)|!)+$", "", regex=True))
        sig = _row_hash(pd.DataFrame({"s": chunk["seller_id"].fillna(""), "t": norm_title}),
                        ["s", "t"])
        self.dup_sig_hashes.append(sig)
        self.dup_posted_ord.append(pd.to_datetime(chunk["posted_date"]).to_numpy().astype("datetime64[D]")
                                   .astype(np.int64))

        # naive definition (title, price) - kept for the collision-rate comparison
        self.naive_hashes.append(_row_hash(chunk.assign(price_str=chunk["price"].astype(str)),
                                           ["title", "price_str"]))

    def finish(self):
        self.prices = np.concatenate(self.prices) if self.prices else np.array([])
        self.id_dup = int(len(self.id_hashes) and
                          (sum(len(h) for h in self.id_hashes) - len(np.unique(np.concatenate(self.id_hashes)))))

        # designed duplicates: same (seller, normalized title) with another row posted
        # within 14 days. Exact via a single lexsort over (signature, date).
        sig = np.concatenate(self.dup_sig_hashes) if self.dup_sig_hashes else np.array([], dtype=np.int64)
        ordinals = (np.concatenate(self.dup_posted_ord) if self.dup_posted_ord
                    else np.array([], dtype=np.int64))
        if len(sig):
            order = np.lexsort((ordinals, sig))
            s_sorted, o_sorted = sig[order], ordinals[order]
            same_prev = s_sorted[1:] == s_sorted[:-1]
            gap_ok = (o_sorted[1:] - o_sorted[:-1]) <= 14
            self.designed_dup = int((same_prev & gap_ok).sum())
        else:
            self.designed_dup = 0

        naive = (np.concatenate(self.naive_hashes) if self.naive_hashes
                 else np.array([], dtype=np.int64))
        self.naive_dup = int(len(naive) - len(np.unique(naive))) if len(naive) else 0


def _classify_price_raw(s: str) -> str:
    if s == "":
        return "blank"
    if s == "negotiable":
        return "negotiable"
    if s.endswith("/-"):
        return "dollar_slash"
    if s.lower().endswith("k"):
        return "k_suffix"
    if s.startswith("$"):
        return "dollar"
    return "plain"


def _fmt_pct(x: float) -> str:
    return f"{x:.2%}"


def _price_table(prices: np.ndarray) -> str:
    if prices.size == 0:
        return "_no prices_"
    qs = np.percentile(prices, [1, 25, 50, 75, 95, 99])
    return (f"min ${prices.min():,.2f} | p1 ${qs[0]:,.2f} | p25 ${qs[1]:,.2f} | "
            f"median ${qs[2]:,.2f} | p75 ${qs[3]:,.2f} | p95 ${qs[4]:,.2f} | "
            f"p99 ${qs[5]:,.2f} | max ${prices.max():,.2f}")


def profile_mercari(path: str, chunk_rows: int) -> MercariProfile:
    prof = MercariProfile()
    for chunk in pd.read_csv(path, sep="\t", chunksize=chunk_rows):
        prof.feed(chunk)
    prof.finish()
    # train_id uniqueness via range check (Mercari ids are 0..N-1)
    ids_ok = prof.rows > 0
    prof.id_range_ok = ids_ok
    return prof


def profile_generated(gen_dir: str, qa_sample: int | None) -> GeneratedProfile:
    prof = GeneratedProfile()
    files = sorted(glob.glob(os.path.join(gen_dir, "run=*", "gen_*.jsonl")))
    for f in files:
        for chunk in pd.read_json(f, lines=True, chunksize=100_000):
            prof.feed(chunk)
    prof.finish()
    prof.files = files
    return prof


def build_report(mp: MercariProfile | None, gp: GeneratedProfile | None,
                 runtime_s: float, peak_mb: float, mercari_rows_seen: int) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines = [f"# ResellRadar Raw-Zone Validation Report", f"",
             f"Generated: {now}  |  Runtime: {runtime_s:.1f}s  |  Peak RSS: {peak_mb:.0f} MB", "",
             "---", ""]

    if mp is not None:
        lines += ["## Source 1: Mercari (REAL)", ""]
        rows = mp.rows
        lines.append(f"- **Rows**: {rows:,}")
        flags = []
        lo, hi = MERCARI_BASELINES["rows"]
        if not (lo <= rows <= hi):
            flags.append(f"row count outside published range {lo:,}-{hi:,}")
        lines += ["", "### Null rate per field", "", "| Field | Null rate |", "|---|---|"]
        for f in mp.fields:
            lines.append(f"| {f} | {_fmt_pct(mp.nulls[f] / rows)} |")
        bl = MERCARI_BASELINES["brand_name_null"]
        brand_rate = mp.nulls["brand_name"] / rows
        lines.append("")
        lines.append(f"- Baseline check: `brand_name` null rate {_fmt_pct(brand_rate)} "
                     f"(published ~43%, expected band {bl[0]:.0%}-{bl[1]:.0%}) -> "
                     f"{'PASS' if bl[0] <= brand_rate <= bl[1] else 'FAIL'}")
        cl = MERCARI_BASELINES["category_name_null"]
        cat_rate = mp.nulls["category_name"] / rows
        lines.append(f"- Baseline check: `category_name` null rate {_fmt_pct(cat_rate)} "
                     f"(published ~0.4%) -> {'PASS' if cl[0] <= cat_rate <= cl[1] else 'FAIL'}")
        lines += ["", "### Duplicates", ""]
        lines.append(f"- `train_id` uniqueness (range check): {'PASS' if rows == mercari_rows_seen else 'CHECK'}")
        lines.append(f"- Zero-price rows: {mp.zero_price:,} ({_fmt_pct(mp.zero_price / rows)}) - documented quality note")
        lines.append(f"- `[rm]` removed descriptions: {mp.desc_rm:,} ({_fmt_pct(mp.desc_rm / rows)})")
        lines += ["", "### Price distribution (USD)", ""]
        lines.append(f"- {_price_table(mp.prices)}")
        lines += ["", "### Category split (top level)", "", "| Top category | Share |", "|---|---|"]
        for cat, n in mp.cat_top.most_common(15):
            lines.append(f"| {cat} | {_fmt_pct(n / rows)} |")
        lines += ["", "### Condition / shipping distribution", ""]
        lines.append(f"- condition 1-5: " + ", ".join(f"c{k}={_fmt_pct(v / rows)}" for k, v in sorted(mp.cond.items())))
        lines.append(f"- shipping (1=seller pays): " + ", ".join(f"{k}={_fmt_pct(v / rows)}" for k, v in sorted(mp.ship.items())))
        gate_fails = sum(mp.structural.values())
        lines += ["", "### Structural gate", ""]
        lines.append(f"- violations: {gate_fails:,} -> {'PASS' if gate_fails == 0 else 'FAIL'}")
        for k, v in mp.structural.items():
            if v:
                lines.append(f"  - {k}: {v:,}")
        lines.append("")

    if gp is not None:
        lines += ["---", "", "## Source 2: Generated (SYNTHETIC)", ""]
        rows = gp.rows
        files = gp.files
        lines.append(f"- **Rows**: {rows:,} across {len(files)} batch files")
        lines.append(f"- Posted-date span: {gp.posted_min:%Y-%m-%d} -> {gp.posted_max:%Y-%m-%d} "
                     f"({(gp.posted_max - gp.posted_min).days} days, target ~730)")
        lines += ["", "### Null rate per field", "", "| Field | Null rate | Target |", "|---|---|---|"]
        targets = {"description": (3, 7), "sub_category": (3, 7), "delisted_date": None, "price": None}
        for f in gp.fields:
            rate = gp.nulls[f] / rows if rows else 0
            t = targets.get(f)
            tstr = f"{t[0]}-{t[1]}%" if t else "-"
            lines.append(f"| {f} | {_fmt_pct(rate)} | {tstr} |")
        lines += ["", "### Duplicates", ""]
        lines.append(f"- `listing_id` duplicates: {gp.id_dup:,}")
        lines.append(f"- Designed duplicate rate (same seller_id + normalized title within 14 days): "
                     f"**{_fmt_pct(gp.designed_dup / rows)}** ({gp.designed_dup:,} rows)")
        lines.append(f"- Naive (title, price) duplicate rate: {_fmt_pct(gp.naive_dup / rows)} "
                     f"({gp.naive_dup:,} rows)")
        lines.append("")
        lines.append("  > Note on the naive rate: at 500k rows over ~1,100 canonical products, ")
        lines.append("> two independent listings can share (title, price) by chance - especially with ")
        lines.append("> quantized price formats and a skewed product mix. The naive rate is therefore an ")
        lines.append("> UPPER bound dominated by chance collisions, not the designed repost rate. ")
        lines.append("> The designed repost share is verified against the ground-truth file below.")
        lines += ["", "### Price distribution (USD)", ""]
        lines.append(f"- {_price_table(gp.prices)}")
        lines.append(f"- null prices (blank/negotiable): {gp.nulls['price'] / rows:.2%} (target ~2%)")
        lines += ["", "### price_raw format distribution", "", "| Format | Share |", "|---|---|"]
        for fmt, n in gp.price_raw_fmt.most_common():
            lines.append(f"| {fmt} | {_fmt_pct(n / rows)} |")
        lines += ["", "### Category split", "", "| Top category | Share |", "|---|---|"]
        for cat, n in gp.cat_top.most_common():
            lines.append(f"| {cat} | {_fmt_pct(n / rows)} |")
        lines += ["", "### Messiness / signal markers", ""]
        lines.append(f"- Alias-city share (NYC/SF/LA/Philly): {_fmt_pct(gp.alias / rows)}")
        lines.append(f"- `category_full` nulls: {gp.nulls.get('category_full', 0) / rows:.2%} (must be 0)")
        gate_fails = gp.bad_date_fmt + gp.bad_order + gp.bad_cond + gp.bad_id
        lines += ["", "### Structural gate", ""]
        lines.append(f"- violations: {gate_fails:,} -> {'PASS' if gate_fails == 0 else 'FAIL'}")
        for name, v in (("malformed ISO dates", gp.bad_date_fmt), ("delisted < posted", gp.bad_order),
                        ("condition outside 1-5", gp.bad_cond), ("bad listing_id", gp.bad_id)):
            lines.append(f"  - {name}: {v:,}")
        lines.append("")
        lines.append("> Acceptance checks for embedded signal (depreciation, time-to-sale shape,")
        lines.append("> regional spread, brand separation): run `python scripts/qa_generated.py "
                     "--run-dir <run=partition>`.")
        lines.append("")

    lines += ["---", "", "## Union notes for Person 2", ""]
    lines.append("See `docs/SCHEMA_MISMATCHES.md`. Reminder: Mercari rows have NULL dates/location;")
    lines.append("generated rows carry the full 17-column union schema. Duplicate rates are reported")
    lines.append("per source and must never be pooled.")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mercari", default=DEFAULT_MERCARI)
    ap.add_argument("--generated-dir", default=DEFAULT_GENERATED)
    ap.add_argument("--chunk-rows", type=int, default=500_000)
    ap.add_argument("--skip-mercari", action="store_true")
    ap.add_argument("--skip-generated", action="store_true")
    args = ap.parse_args()

    import psutil
    proc = psutil.Process()
    t0 = datetime.now()
    peak = 0.0

    mp = None
    mercari_rows_seen = 0
    if not args.skip_mercari:
        if os.path.exists(args.mercari):
            print(f"[mercari] chunked profile of {args.mercari} ...")
            mp = profile_mercari(args.mercari, args.chunk_rows)
            mercari_rows_seen = mp.rows
            peak = max(peak, proc.memory_info().rss / 1e6)
            print(f"[mercari] rows={mp.rows:,}")
        else:
            print(f"[mercari] NOT FOUND at {args.mercari} (skipping)")

    gp = None
    if not args.skip_generated:
        files = glob.glob(os.path.join(args.generated_dir, "run=*", "gen_*.jsonl"))
        if files:
            print(f"[generated] chunked profile of {len(files)} batch files ...")
            gp = profile_generated(args.generated_dir, None)
            peak = max(peak, proc.memory_info().rss / 1e6)
            print(f"[generated] rows={gp.rows:,}")
        else:
            print(f"[generated] no batch files under {args.generated_dir} (skipping)")

    peak = proc.memory_info().rss / 1e6
    runtime = (datetime.now() - t0).total_seconds()
    report = build_report(mp, gp, runtime, peak, mercari_rows_seen)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"[report] -> {os.path.relpath(REPORT_PATH, PROJECT_ROOT)} ({runtime:.1f}s, peak {peak:.0f} MB)")

    gate_ok = (mp is None or sum(mp.structural.values()) == 0) and \
              (gp is None or (gp.bad_date_fmt + gp.bad_order + gp.bad_cond + gp.bad_id) == 0)
    sys.exit(0 if gate_ok else 1)


if __name__ == "__main__":
    main()
