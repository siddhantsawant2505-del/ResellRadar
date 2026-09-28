# Person 1's Work, Explained in Plain Language

**Who this is for**: anyone on the team (or grading the project) who wants to understand
what Person 1 built and why - without wading through code.

**The one-paragraph story**: our project studies second-hand marketplace listings (phones
and furniture). Real data alone didn't have everything we need (the real Mercari dataset
has prices and categories but no dates or locations), so Person 1 (a) brought in the real
data, (b) built a "fake data factory" that manufactures realistic listings WITH dates and
locations, (c) moved both datasets onto a real Hadoop storage cluster, and (d) wrapped
everything with quality checks, tamper-evident seals, and an "answer key" so the teammates
doing the actual analysis can prove their work is correct.

---

## Step 0 - Setting up the workspace

Before any data work: made sure the project's data folders (which hold gigabytes) are
never uploaded to GitHub, installed the tools needed (Kaggle downloader, archive reader,
memory profiler), and parked the old first-version code (a web scraper) since the team
switched to using a ready-made dataset instead of scraping websites.

## Step 1 - Getting the real data

Downloaded the **Mercari Price Suggestion** dataset from Kaggle: about **1.48 million real
second-hand listings** (title, description, price, category, condition). It was downloaded
as a compressed archive, verified with a checksum (a digital fingerprint proving the file
wasn't corrupted), extracted to `data/raw/mercari/train.tsv`, and never modified again.

Think of the raw zone like a museum archive: once something goes in, nobody is allowed to
edit it. All future work reads from it; nothing writes back to it.

## Step 2 - Taking a small sample to learn from

1.48 million rows is too big to study by hand, so a script picked out just the phone and
furniture listings (`data/seed/mercari_phone_furniture_sample.csv`, ~8,900 rows). This
sample acts like a reference photo: it tells the fake-data factory what real listings look
like - which brands are common (Apple is ~70% of phones), what prices look like, how the
condition of items is distributed. The factory then imitates reality instead of inventing
random nonsense.

## Step 3 - Writing the contract before building

Three "schema" documents were written BEFORE the factory was coded, like blueprints
before construction:

- `docs/SCHEMA_mercari.md` - what the real data contains.
- `docs/SCHEMA_generated.md` - what the fake data will contain.
- `docs/SCHEMA_MISMATCHES.md` - where the two differ and how to combine them
  (for example: the real data has NO dates at all; the fake data has dates on every row).

Because Person 2's code will mix both datasets, agreeing on the blueprint first prevents
the classic group-project disaster where two people's code expects different things.

## Step 4 - The fake-data factory (the biggest single piece)

`generator/generate_data.py` manufactured **500,000 synthetic listings** in about
10 seconds. "Synthetic" means computer-generated, but every row follows realistic rules
learned from the real sample in Step 2:

- **1,108 distinct products.** Products are combinations like
  "Apple iPhone 13, 128GB, Midnight Black" (phone: brand x model x storage x 8 colors) or
  "Living Room Leather Sofa, Metal frame, Compact size" (furniture: type x material x
  size). 688 phone products + 420 furniture products.
- **50,000 fake sellers, realistically lopsided.** A few "power sellers" post thousands of
  listings; most sellers post only a few - just like a real marketplace. Each listing
  carries a `seller_id`.
- **Deliberate messiness.** Real data is messy, so ours is too: prices written five
  different ways ("$1,200", "1200", "$1.2k", "$1,200/-", or blank), some missing
  descriptions, city names sometimes written as nicknames (NYC instead of New York),
  ~1% bait prices. Cleaning this mess up is exactly the exercise Person 2 is graded on.
- **Hidden storylines for the analysis.** Prices drop as items age; items sell after a
  realistic, random-ish number of days; some cities pay more than others. These planted
  patterns are what Person 3's analytics are supposed to rediscover.
- **Planted duplicates ("reposts").** Exactly 10% of listings are secret near-duplicates:
  the same seller re-listing the same item a few days later with a slightly different
  title ("... - must go") and a slightly different price. Finding these duplicates is
  Person 2's main technical challenge (entity resolution).
- **The answer key.** The truth about every listing - which product it really is, whether
  it's a repost, and which listing it duplicates - is written to a SEPARATE file,
  `data/ground_truth/run=<timestamp>/truth.parquet`. It is deliberately kept OUT of the
  raw data (no cheating) and works like a teacher's answer sheet: after Person 2's
  duplicate-finding code runs, we compare its answers against this key to grade it.
- **Reproducible.** The factory uses a fixed random seed (42). We proved this by
  regenerating everything a second time: all 50 data files came out **byte-for-byte
  identical**. Same seed, same data - every time.

The 500,000 rows are written as 50 files of 10,000 rows each into
`data/raw/generated/run=2026_09_28T150245Z/`, with the timestamp in the folder name so a
re-run never overwrites an older batch.

## Step 5 - Quality control (two layers)

- **Structural gate** (`scripts/validate_raw.py`): hard rules that must never break -
  dates in valid format, sell-date never before list-date, condition between 1 and 5,
  every row has an ID. Zero violations on all ~2 million rows.
- **Statistical report**: the soft checks - are null rates where we designed them, do
  prices look right, does the real data match published statistics? (It does: the real
  data's missing-brand rate came out 42.68%, matching the officially known ~43% - proof
  we parsed it correctly.) Output: `logs/validation_report.md`.
- **Deeper factory checks** (`scripts/qa_generated.py`): 27 automated checks on the
  synthetic data - all 27 pass (right product count, exactly 10% reposts, reposts really
  share their seller, dates really 1-14 days apart, no broken links in the answer key...).

## Step 6 - Moving into the "warehouse" (real Hadoop)

HDFS is the storage system of big-data tools - Person 2's Spark jobs and Person 3's
analytics are supposed to read from it. Earlier in the project, an "emulated" (pretend)
version was used because no cluster existed. This phase replaced pretend with real:

- Wrote `docker-compose.yml` + config, which starts a genuine (small) Hadoop cluster on
  this PC inside Docker: one **NameNode** (the librarian who knows where everything is)
  and one **DataNode** (the shelf that holds the actual bytes), using the official
  Apache Hadoop image.
- Team rules honored: each file stored once (replication factor 1) and cut into
  64 MB blocks.
- Uploaded both datasets with `scripts/push_to_hdfs.py`. The script now has a strict
  policy: if the cluster isn't running, it **refuses loudly** - it will never silently
  pretend to have pushed anything (the old fake mode was deleted on purpose).
- Captured proof into `logs/hdfs_proof.txt`: directory listings, sizes, and a full
  block-by-block health scan of everything under `/data/raw`, which came back
  **HEALTHY, zero missing or corrupted blocks**. 51 files, 650.7 MB, 56 blocks.

Setup instructions for reproducing this on any Windows PC: `docs/HDFS_SETUP.md`.

## Step 7 - Tamper-evident seals

`logs/checksums.txt` stores a SHA-256 fingerprint for every data file (the real TSV, all
50 generated files, and the answer key). Anyone can re-run the checksum command later and
immediately see whether even one byte changed. The real dataset's fingerprint matches the
one recorded on download day - unchanged.

## Step 8 - The handoff package

- `docs/HANDOFF_Person2.md` - everything Person 2 needs: where the data lives (on disk and
  on the cluster), what changed in the data contract, the rules for using the answer key.
- `scripts/evaluate_er.py` - the **grader**: Person 2 runs their duplicate-detection
  output through it and gets precision/recall/F1 scores against the answer key
  (tested: perfect answers score exactly 100%).
- `docs/PERSON1_TASKS_EXPLAINED.md` - this document.

---

## Where everything lives

| Thing | Location |
|---|---|
| Real data (immutable) | `data/raw/mercari/train.tsv` (1,482,535 rows) |
| Fake data (immutable) | `data/raw/generated/run=2026_09_28T150245Z/` (50 files, 500,000 rows) |
| Answer key | `data/ground_truth/run=2026_09_28T150245Z/truth.parquet` |
| The factory | `generator/generate_data.py` (+ `generator/calibration.py`) |
| Quality checks | `scripts/validate_raw.py`, `scripts/qa_generated.py` |
| Warehouse uploader | `scripts/push_to_hdfs.py` + `docker-compose.yml` |
| ER grader | `scripts/evaluate_er.py` |
| Evidence | `logs/checksums.txt`, `logs/validation_report.md`, `logs/hdfs_proof.txt`, `logs/ingestion.log` |
| Documents | `docs/SCHEMA_*.md`, `docs/HDFS_SETUP.md`, `docs/HANDOFF_Person2.md` |

## Status snapshot (2026-09-28)

| Metric | Value |
|---|---|
| Real rows | 1,482,535 (Mercari, checksum-pinned) |
| Synthetic rows | 500,000 (+ 500,000-row answer key) |
| Factory speed | ~51,000 rows/s, ~10 s for a full run, seed 42 |
| QA checks | 27/27 pass |
| Validation gate | 0 structural violations across ~2M rows |
| HDFS | real 2-node (NN+DN) cluster, 51 files / 650.7 MB / 56 blocks, fsck HEALTHY |
| Planted duplicates | exactly 10.0%, all sharing seller_id, gaps 1-14 days |
