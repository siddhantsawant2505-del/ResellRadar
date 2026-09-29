# Real HDFS on Docker (Windows 11) — setup & run guide

Single-node **pseudo-distributed HDFS** in Docker: one NameNode + one DataNode using the
official `apache/hadoop:3` image (3.3.6). This replaces the earlier emulated push —
there is **no emulation fallback**: if no live cluster is reachable,
`scripts/push_to_hdfs.py` exits non-zero with a loud error.

## Cluster contract (team decision)

| Setting | Value | Where enforced |
|---|---|---|
| Replication factor | **1** | `docker/hadoop/hdfs-site.xml` (`dfs.replication`) |
| Block size | **64 MB** (67,108,864 B) | `docker/hadoop/hdfs-site.xml` (`dfs.blocksize`) |
| NameNode storage | volume `hdfs_namenode` → `/hadoop/dfs/name` | `hdfs-site.xml` + compose volumes |
| DataNode storage | volume `hdfs_datanode` → `/hadoop/dfs/data` | `hdfs-site.xml` + compose volumes |
| Permissions | off (single-user mini cluster) | `dfs.permissions.enabled=false` |

Ports on localhost: **9870** NameNode Web UI / WebHDFS, **9000** HDFS RPC, 9864 DataNode UI.

## Prerequisites (one-time)

1. **Docker Desktop** with the WSL-2 backend (already installed on this machine;
   docker.exe lives at `C:\Program Files\Docker\Docker\resources\bin\docker.exe` — add it
   to PATH or use the full path). Verify: `docker version` → Server version prints.
2. Nothing else — Hadoop itself runs entirely inside the containers.

## Start the cluster

```bash
docker compose -f docker-compose.yml up -d          # first start formats the NameNode once
docker ps                                           # wait for: namenode (healthy), datanode (up)
docker exec resellradar-namenode hdfs dfsadmin -report   # "Live datanodes (1)"
```

The NameNode healthcheck asks whether the HDFS RPC answers (`dfsadmin -report`), not whether
safe mode is off - safe mode can only lift *after* the DataNode registers, so a
safemode-based check deadlocked every restart with data on disk (the DataNode sat waiting
for `service_healthy`). With the RPC check, plain `up -d` brings the whole stack up; safe
mode lifts by itself seconds after the DataNode's block report lands.

Web UI: http://localhost:9870 → Utilities ▸ Browse the file system.

## Push the raw zone (real upload)

```bash
python scripts/push_to_hdfs.py --fresh        # --fresh: ignore stale sync manifests
```

- Mode is auto-detected: HDFS CLI via `docker exec` (preferred) → WebHDFS → local `hdfs`.
  The docker-CLI path wins because host-side WebHDFS writes get redirected to the
  datanode's container hostname, which Windows can't resolve.
- Uploads `data/raw/mercari/` → `/data/raw/mercari/` and
  `data/raw/generated/run=<ts>/` → `/data/raw/generated/run=<ts>/` (partition preserved).
- Every file is logged to `logs/ingestion.log` with record count, bytes, and block count.

## Capture the proof artifact

```bash
# IMPORTANT (Git Bash/MSYS): disable path mangling or '/data/raw' becomes 'C:/.../data/raw'
export MSYS2_ARG_CONV_EXCL="*" MSYS_NO_PATHCONV=1
docker exec resellradar-namenode hdfs dfs -ls -R /data/raw
docker exec resellradar-namenode hdfs dfs -du -h /data/raw
docker exec resellradar-namenode hdfs fsck /data/raw -files -blocks
```

The committed snapshot of all three commands lives in `logs/hdfs_proof.txt`
(ends with `The filesystem under path '/data/raw' is HEALTHY`).

## Land the processed and curated zones (full medallion lake)

The Spark stages write to the host disk (`data/processed/`, `data/curated/`) because they
run through the Spark container with the repo bind-mounted. To complete the three-zone lake
on the cluster, push the stage outputs up:

```bash
python scripts/push_zones_to_hdfs.py        # processed + curated + chatter_vocab.json
python scripts/push_zones_to_hdfs.py --force   # re-push after a pipeline re-run
```

- Pushes parquet **directory tables** as whole directories, so they stay readable as
  tables (`spark.read.parquet("hdfs://namenode:9000/data/processed/entity_resolved.parquet")`).
- Re-runs skip unchanged entries (manifest: `data/hdfs_zone_sync_manifest.json`); a re-push
  replaces the remote path (these zones are rebuildable artifacts — the raw zone stays the
  immutable one, and this script never touches it).
- Per-zone rows/bytes/blocks land in `logs/hdfs_zone_ingestion.txt`.

Read-back proof (full scans from inside the cluster network — also proves blocks are
healthy):

```bash
bash scripts/run_stage_in_docker.sh scripts/verify_hdfs_zones.py
```

Committed snapshot: `logs/hdfs_zone_readback.txt`. Expected (2026-09-29 run): processed
1,972,679 rows (clean 21 cols / entity_resolved 24 cols), curated 1,062,905 / 1,105 /
1,056,069 rows.

Full HDFS tree after all three zone pushes: `/data/raw` (620.6 MB, immutable) →
`/data/processed` (~787 MB) → `/data/curated` (~17.5 MB).

## Stop / reset

```bash
docker compose -f docker-compose.yml down        # stop; volumes keep the data
docker compose -f docker-compose.yml down -v     # stop AND wipe HDFS (fresh cluster)
```

If containers won't start: `docker logs resellradar-namenode` first. The classic
failure is an unformatted NameNode (`InconsistentFSStateException`) — the compose
command formats once when `/hadoop/dfs/name/current/VERSION` is missing, so this only
happens if the volume was wiped mid-flight; `down -v` + `up -d` resets cleanly.

## Fail-loudly behavior

`scripts/push_to_hdfs.py` probes the cluster before pushing anything. If neither the
docker-exec CLI, WebHDFS, nor a local `hdfs` binary answers, it prints a FATAL banner
with startup instructions and exits with code **2** — it never cataloged files as
"pushed" without a real cluster (the old EMULATED mode is gone).
