#!/usr/bin/env python
"""ResellRadar - learn the Stage 2 chatter vocabulary from the corpus itself.

Person 1: replaces the fixed 27-token chatter list that entity_resolution_v2.py
hardcodes (COND_WORDS + TITLE_EXTRAS + TEMPLATE_FILLERS) with a vocabulary
DERIVED FROM THE DATA, so entity merging generalizes beyond the generator's
word list to real-world titles (Mercari).

METHOD - removability signal
----------------------------
A token is *chatter* iff deleting it from a title yields the exact sorted token
set of another real title in the same source. Rationale:
  * condition words / template fillers ("good", "with", "pickup", "must go")
    are surface-optional: plenty of sibling titles exist without them, so the
    deletion lands on a real title;
  * product tokens are NOT optional: every phone title names its colour and
    storage, so deleting "titanium" or "128gb" yields a title that simply does
    not exist. Deleting to an EXACT set (not a subset) also avoids the
    accessory trap, e.g. {apple, iphone, 13} is a subset of many case-title
    sets but deleting it must land exactly on a real case title, which does
    not happen at meaningful rates.
Tokens are learned per source_platform (generated and mercari chatter differ).
Removability is tested for 1-, 2- and 3-token contiguous phrases so phrases
like "screen protector on" are caught even when the single words are not.
A token counts as chatter if it is removable in >= --rate-thr of the titles
that contain it (and it occurs in >= --min-df distinct titles).

SEMANTIC GUARD (the only hand-written piece)
--------------------------------------------
GUARD lists product-line suffixes ("pro", "max", "plus", "mini", "ultra", ...).
They are surface-optional in text, so the removability statistics mark them
chatter (measured: "pro" removable in ~50% of containing titles) - but they are
catalog identity: dropping them merges iPhone 15 Pro with iPhone 15 Pro Max.
GUARD is therefore never removable regardless of statistics. Everything else
is learned. The guard is subtracted before the vocabulary is written.

NO GROUND-TRUTH ACCESS: this script reads only the Stage 1 output
(data/processed/clean_listings.parquet). The truth file is never read, at
learn time or at job time.

OUTPUT
------
data/processed/chatter_vocab.json - consumed by spark_jobs/entity_resolution_v3.py,
which fails loudly if the artifact is missing (run this script first).

Usage:
    python scripts/learn_chatter_vocab.py
    python scripts/learn_chatter_vocab.py --min-df 50 --rate-thr 0.85 \
        --max-phrase 3 --out logs/learn_chatter_vocab.txt
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent

DEFAULT_INPUT = BASE_DIR / "data" / "processed" / "clean_listings.parquet"
DEFAULT_OUTPUT = BASE_DIR / "data" / "processed" / "chatter_vocab.json"

# Product-line suffixes: surface-optional, catalog-identity. Never removable.
SEMANTIC_GUARD = [
    "pro", "max", "plus", "mini", "ultra", "se", "air", "edge", "note", "fe", "lite",
]

# Diagnostic only (reporting, not learning): the fixed 27-token list v2 hardcodes,
# as documented in docs/SCHEMA_generated.md. We report whether the learned
# vocabulary rediscovers it; the vocabulary itself never sees this list.
DOCUMENTED_CHATTER = {
    "for", "parts", "poor", "fair", "good", "like", "new",
    "with", "box", "clean", "imei", "no", "scratches", "screen", "protector",
    "on", "factory", "unlocked", "esim", "ready",
    "used", "fs", "selling", "pickup", "must", "go", "relist",
}


def load_distinct_title_sets(path: Path) -> dict:
    """Distinct (source_platform, token-set) pairs from the Stage 1 output."""
    df = pd.read_parquet(path, columns=["title_clean", "source_platform"])
    df = df.dropna(subset=["title_clean", "source_platform"]).drop_duplicates()
    out = {}
    for source, sub in df.groupby("source_platform"):
        out[str(source)] = [frozenset(t.split()) for t in sub["title_clean"]]
    return out


def _combinations(items, k):
    if k == 0:
        yield ()
        return
    for i in range(len(items)):
        for rest in _combinations(items[i + 1:], k - 1):
            yield (items[i],) + rest


def learn(title_sets, min_df=50, rate_thr=0.85, max_phrase=3, guard=frozenset()):
    """Return (vocabulary, stats) for one source's distinct title token-sets."""
    exact = set(" ".join(sorted(t)) for t in title_sets)   # O(1) deletion lookups
    df_count = defaultdict(int)
    for toks in title_sets:
        for w in toks:
            df_count[w] += 1
    candidates = {w for w, c in df_count.items() if c >= min_df}

    removable = defaultdict(int)   # token -> titles where some deletion covering it hit a real title
    seen = defaultdict(int)        # token -> titles containing it (among candidates)
    for toks in title_sets:
        active = toks & candidates
        if not active:
            continue
        lst = sorted(toks)         # sorted once: any subset deletion is a contiguous slice
        n = len(lst)
        idx = list(range(n))
        marked = set()
        for size in range(1, max_phrase + 1):
            if len(marked) == len(active):
                break
            for combo in _combinations(idx, size):
                ws = [lst[i] for i in combo]
                if not any(w in active and w not in marked for w in ws):
                    continue
                keep = " ".join(lst[i] for i in idx if i not in combo)
                if keep in exact:
                    marked.update(ws)
        for w in marked:
            removable[w] += 1
        for w in active:
            seen[w] += 1

    vocab = {
        w for w in candidates
        if seen[w] and removable[w] / seen[w] >= rate_thr
    } - guard

    stats = {
        "distinct_titles": len(title_sets),
        "candidate_tokens": len(candidates),
        "vocabulary_size": len(vocab),
        "top_removed": sorted(
            ((w, removable[w], seen[w]) for w in vocab),
            key=lambda x: -(x[1] / x[2] if x[2] else 0),
        )[:0],  # placeholder, not used in output
    }
    return vocab, stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Learn the Stage 2 chatter vocabulary from the corpus.")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    ap.add_argument("--min-df", type=int, default=50,
                    help="minimum number of distinct titles a token must occur in")
    ap.add_argument("--rate-thr", type=float, default=0.85,
                    help="minimum fraction of containing titles where deletion yields another real title")
    ap.add_argument("--max-phrase", type=int, default=3,
                    help="longest contiguous token phrase tested for deletion")
    args = ap.parse_args()

    if not args.input.exists():
        print(f"ERROR: input parquet not found: {args.input}", file=sys.stderr)
        print("Run Stage 1 (spark_jobs/clean_normalize.py) first.", file=sys.stderr)
        return 2

    print("Learning chatter vocabulary (removability method)")
    print(f"  input     : {args.input}")
    print(f"  output    : {args.output}")
    print(f"  min_df    : {args.min_df}")
    print(f"  rate_thr  : {args.rate_thr}")
    print(f"  max_phrase: {args.max_phrase}")
    print(f"  guard     : {sorted(SEMANTIC_GUARD)}")
    print()

    title_sets_by_source = load_distinct_title_sets(args.input)

    vocabularies = {}
    stats = {}
    for source in sorted(title_sets_by_source):
        vocab, st = learn(
            title_sets_by_source[source],
            min_df=args.min_df,
            rate_thr=args.rate_thr,
            max_phrase=args.max_phrase,
            guard=frozenset(SEMANTIC_GUARD),
        )
        vocabularies[str(source)] = sorted(vocab)
        stats[str(source)] = {
            "distinct_titles": st["distinct_titles"],
            "candidate_tokens": st["candidate_tokens"],
            "vocabulary_size": st["vocabulary_size"],
        }
        print(f"[{source}]")
        print(f"  distinct titles      : {st['distinct_titles']}")
        print(f"  candidate tokens     : {st['candidate_tokens']}")
        print(f"  learned chatter tokens: {st['vocabulary_size']}")
        print(f"  vocabulary           : {' '.join(vocabularies[source])}")
        print()

    learned_all = set().union(*vocabularies.values()) if vocabularies else set()
    guard_overlap = learned_all & set(SEMANTIC_GUARD)
    print(f"guard tokens present in learned vocab (must be 0): {len(guard_overlap)}")

    gen = vocabularies.get("generated", [])
    rediscovered = sorted(DOCUMENTED_CHATTER & set(gen))
    missing = sorted(DOCUMENTED_CHATTER - set(gen))
    print(f"\ndiagnostic: learned (generated) covers documented v2 list: "
          f"{len(rediscovered)}/{len(DOCUMENTED_CHATTER)}")
    if missing:
        print(f"  missing: {missing}")
    print(f"diagnostic: learned (generated) beyond the documented list: "
          f"{len(set(gen) - DOCUMENTED_CHATTER)} tokens (city names etc.)")

    payload = {
        "version": 1,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input": str(args.input.relative_to(BASE_DIR))
        if args.input.is_relative_to(BASE_DIR) else str(args.input),
        "method": (
            "removability: a token/phrase is chatter iff deleting it from a title yields "
            "the exact sorted token set of another real title from the same source; "
            "tokens removable in >= rate_thr of containing titles (min_df distinct-title "
            "support) are chatter. Per-source vocabularies. Semantic guard (product-line "
            "suffixes) is never removable. No ground truth used."
        ),
        "parameters": {
            "min_df": args.min_df,
            "removal_rate_threshold": args.rate_thr,
            "max_phrase_length": args.max_phrase,
        },
        "semantic_guard": sorted(SEMANTIC_GUARD),
        "stats": stats,
        "vocabularies": vocabularies,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nwrote {args.output}")
    for source in sorted(vocabularies):
        print(f"  {source}: {len(vocabularies[source])} tokens")
    return 0


if __name__ == "__main__":
    sys.exit(main())
