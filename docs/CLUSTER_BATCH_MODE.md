# ResellRadar — distributed batch processing (Spark Standalone cluster)

The pipeline no longer has to run as one standalone processing unit. There are now two
execution modes; the job files are identical, the data is identical, and the results are
identical (verified to the exact row count on 2026-10-05 — see "Proof" below).

| | Single-unit (default) | Distributed batch (cluster profile) |
|---|---|---|
| Compute | 1 container, `local[*]` (16 host cores in one JVM) | Spark Standalone master + 3 workers, 3 executor JVMs x 3 cores (9 cores total) |
| Storage | HDFS zones (inputs) + local `data/` (outputs) | HDFS zones for inputs AND outputs (`hdfs://namenode:9000/data/...`) |
| Submission | `scripts/run_stage_in_docker.sh <job>` | `CLUSTER=1 scripts/run_stage_in_docker.sh <job>` — or just click **Start pipeline** on the dashboard (auto-detects the cluster; see below) |

## Start / stop the batch cluster

```bash
# start (HDFS + Spark master + 3 workers; the "cluster" profile keeps the default
# `docker compose up -d` stack unchanged — storage-only, as before)
docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3

# Spark Master Web UI (workers, applications, cores in use): http://localhost:8080

# run the whole pipeline distributed (detached, one container per stage)
CLUSTER=1 DETACH=1 CONTAINER_NAME=resellradar-spark-s1 bash scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
CLUSTER=1 DETACH=1 CONTAINER_NAME=resellradar-spark-s2 bash scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
CLUSTER=1 DETACH=1 CONTAINER_NAME=resellradar-spark-s3 bash scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
# verify all 5 HDFS tables (also distributed)
CLUSTER=1 DETACH=1 CONTAINER_NAME=resellradar-spark-verify bash scripts/run_stage_in_docker.sh scripts/verify_hdfs_zones.py

# stop
docker compose -f docker-compose.yml --profile cluster down
```

## How it works (and why the job files barely changed)

- `spark-submit --master spark://spark-master:7077` + `SPARK_MASTER` env override the
  jobs' session master (an explicitly set `.master(...)` in code beats the CLI flag, so
  the jobs read `SPARK_MASTER` with fallback `local[*]`).
- `RR_DATA_ROOT` (set to `hdfs://namenode:9000/data` by the harness in cluster mode)
  redirects the jobs' local `data/processed|curated` paths to the matching HDFS zones.
  Default (unset) keeps the original local-checkout behavior.
- Executors live in the `spark-worker` containers, which bind-mount the repo read-only
  at `/work` so job modules import correctly.
- The driver container is named/hostnamed so executors can connect back to it
  (`spark.driver.host` = container hostname, resolvable on the compose network).
- Cluster jobs run as root because the HDFS `/data` zones are owned by `root:supergroup`.
- The harness passes `--conf spark.hadoop.dfs.replication=1` because this client has no
  `hdfs-site.xml` (otherwise HDFS writes use the built-in default replication 3 on our
  replication-1 cluster — fixed by setrep once, prevented by the conf since then).

## Sizing (fits the Docker Desktop VM: 16 CPU / ~7.6 GB RAM shared with Hadoop)

| Component | Allocation |
|---|---|
| spark-master | 512 MB daemon heap |
| spark-worker x3 | 3 cores + 3 GiB advertised each; 256 MB worker daemon heap |
| executor (per worker) | 3 cores, 1 GiB heap |
| driver (client container) | 1.5 GiB heap |

Measured peak during Stage 2 (heaviest shuffles): ~3.3 GB used across all containers.
Knobs: `EXECUTOR_MEMORY`, `EXECUTOR_CORES`, `DRIVER_MEMORY`, `SPARK_MASTER_URL`,
`RR_DATA_ROOT` (see `scripts/run_stage_in_docker.sh` header). To grow the cluster:
`--scale spark-worker=N` (add host RAM via `%UserProfile%\.wslconfig` if you push past
the WSL2 50%-of-RAM default).

## Proof (2026-10-05 run, `logs/cluster_batch_run.txt`)

- Master registered **3 workers x 3 cores**; all 4 applications registered on the
  standalone master (`ResellRadar-CleanNormalize`, `-EntityResolution`,
  `-FeatureEngineering`, `-VerifyHdfszones`).
- Each stage log shows **3 executors registered** from 3 distinct worker IPs
  (`logs/cluster_stage1.txt`, `cluster_stage2.txt`, `cluster_stage3.txt`, `cluster_verify.txt`).
- Distributed results, exit code 0 everywhere, counts identical to the accepted
  single-unit run: raw 1,982,535 → clean **1,972,679**; unique entities **1,040,681**;
  reposts **49,788**; curated **1,062,905 / 1,105 / 1,056,069**.
- Read-back of all 5 HDFS tables distributed: exact expected rows/cols
  (`logs/cluster_verify.txt`); `hdfs fsck /data` → HEALTHY.
- Single-unit regression: default harness run still executes `local[*]` with local
  outputs, exit 0 (`logs/local_regression_stage3.txt`).

## Triggering it from the dashboard ("Start pipeline")

The dashboard's **Start pipeline** button (POST :3000/api/scrape/trigger -> backend `server.py`)
automatically uses the multiple worker units whenever the cluster profile is up:

- On trigger the backend probes for `resellradar-spark-master`. Up -> `execution_mode =
  "distributed (Spark Standalone: 3 worker units x 3 executor cores)"`; down -> Spark stages
  4-6 fall back to host-Python `local[*]` with a WARN line in the log stream.
- In distributed mode each Spark stage is submitted as a **driver container** named
  `resellradar-spark-console-<idx>-<stage>-<ts>` on the `resellradar_default` network
  (image `resellradar/spark:4.2.0-numpy`, same sizing/conf as the harness: 1536m driver,
  1g executors x 3 cores, `dfs.replication=1`, `RR_DATA_ROOT=hdfs://namenode:9000/data`).
  Executors run on the worker units and read/write the HDFS zones, exactly like
  `CLUSTER=1` manual submissions.
- Before Stage 7 (acceptance) the backend syncs the 5 HDFS output tables back to the local
  checkout (`sync_hdfs_outputs_to_local()`: copyToLocal inside namenode -> docker cp ->
  `os.replace`), so acceptance always grades the outputs the distributed run just wrote.
- **Stop** terminates the driver containers too (`resellradar-spark-console-*`), not just the
  local subprocess.

### Batch cluster panel (`/api/cluster` -> ClusterPanel on the dashboard)

`GET :3000/api/cluster` returns the live cluster view rendered as an execution-flow diagram
(driver -> Spark master -> one card per worker unit) under the "Batch cluster" section:

- `workers[]` — per worker unit: container status, live CPU %, memory used/limit
  (`docker stats`), spinner while a stage is executing.
- `running_apps[]` — live `resellradar-spark-console-*` driver containers (the running stage).
- `finished_apps[]` — the run's completed stages with FINISHED/FAILED state.
  (Spark 4's master web UI serves no JSON REST, so app tracking uses infrastructure we
  control: live driver containers + the backend's per-stage history.)

### Dashboard-triggered distributed run — evidence (2026-10-05 ~20:28-20:37 IST)

- Triggered via POST :3000/api/scrape/trigger; auto-detected distributed mode, Stage 4
  driver `resellradar-spark-console-4-spark-clean---normalize-*` while all **3 worker cards
  sat at ~305% CPU** (3 cores each) on the Batch cluster panel.
- Stages 4, 5, 6 all recorded **FINISHED**; `[OK] HDFS stage outputs synced`;
  Stage 7 acceptance PASS 24/24 with the canonical counts (clean 1,972,679; entities
  1,040,681; reposts 49,788; curated 1,062,905 / 1,105 / 1,056,069);
  `=== PIPELINE FINISHED SUCCESSFULLY in 493.44s ===`; workers back to ~0.1% CPU after.

## Caveats

- Workers are Docker containers on one host: it is a real multi-unit Spark cluster
  (separate executor JVMs, separate schedulable cores, HDFS-backed I/O), but not
  physical machines — data locality is 1 DataNode deep.
- The F3 note from `spark_jobs/entity_resolution.py` still applies: the entity-id
  `row_number()` flows ~1.04M keys through one partition (it completed in seconds on
  one executor).
