#!/usr/bin/env python
"""ResellRadar - land the processed and curated zones on the real HDFS cluster.

The raw zone (/data/raw) is landed by scripts/push_to_hdfs.py. This script completes the
three-zone lake: it uploads the Spark stages' parquet outputs and the learned chatter
vocabulary so the whole medallion layout lives on the cluster, not only on the host disk:

    data/processed/clean_listings.parquet      -> /data/processed/clean_listings.parquet
    data/processed/entity_resolved.parquet     -> /data/processed/entity_resolved.parquet
    data/processed/chatter_vocab.json          -> /data/processed/chatter_vocab.json
    data/curated/*_curated.parquet             -> /data/curated/<same name>

Parquet outputs are Spark "directory tables" (one or more part-*.snappy.parquet files plus
a _SUCCESS marker); they are pushed as whole DIRECTORIES so they remain readable as tables
(spark.read.parquet("hdfs://namenode:9000/data/processed/entity_resolved.parquet")).

The same connection-mode ladder as push_to_hdfs.py: docker-exec HDFS CLI first (the host
cannot reach the DataNode transfer port), then WebHDFS, then a local hdfs binary. No
emulation fallback - fails loudly when no live cluster is reachable.

Re-runs skip entries whose local size is unchanged (manifest:
data/hdfs_zone_sync_manifest.json); pass --force to re-push everything. Since a directory
cannot be atomically replaced in HDFS, a re-push removes the remote path first and puts the
new copy - these zones are rebuildable pipeline artifacts, never primary records (the raw
zone is the immutable one, and this script never touches it).

Usage:
    python scripts/push_zones_to_hdfs.py [--namenode http://localhost:9870] [--force] [--fresh]
"""

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ZONE_SOURCES = [
    (os.path.join(PROJECT_ROOT, "data", "processed", "clean_listings.parquet"),
     "/data/processed/clean_listings.parquet"),
    (os.path.join(PROJECT_ROOT, "data", "processed", "entity_resolved.parquet"),
     "/data/processed/entity_resolved.parquet"),
    (os.path.join(PROJECT_ROOT, "data", "processed", "chatter_vocab.json"),
     "/data/processed/chatter_vocab.json"),
    (os.path.join(PROJECT_ROOT, "data", "curated", "depreciation_curve_curated.parquet"),
     "/data/curated/depreciation_curve_curated.parquet"),
    (os.path.join(PROJECT_ROOT, "data", "curated", "resale_velocity_curated.parquet"),
     "/data/curated/resale_velocity_curated.parquet"),
    (os.path.join(PROJECT_ROOT, "data", "curated", "regional_price_variance_curated.parquet"),
     "/data/curated/regional_price_variance_curated.parquet"),
]

LOG_PATH = os.path.join(PROJECT_ROOT, "logs", "hdfs_zone_ingestion.txt")
MANIFEST_PATH = os.path.join(PROJECT_ROOT, "data", "hdfs_zone_sync_manifest.json")

NAMENODE_CONTAINER = "resellradar-namenode"
BLOCK_SIZE = 64 * 1024 * 1024
REPLICATION = 1
SUCCESS_MARKER = "_SUCCESS"


def _resolve_docker() -> str:
    found = shutil.which("docker")
    if found:
        return found
    for cand in (r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
                 "/usr/bin/docker", "/usr/local/bin/docker"):
        if os.path.exists(cand):
            return cand
    return "docker"


DOCKER = _resolve_docker()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fail_loudly(reason: str) -> None:
    print("=" * 72, file=sys.stderr)
    print("FATAL: no live HDFS cluster reachable - zone push aborted.", file=sys.stderr)
    print(f"Reason: {reason}", file=sys.stderr)
    print("", file=sys.stderr)
    print("Start the real single-node cluster first (no emulation fallback exists):", file=sys.stderr)
    print("    docker compose -f docker-compose.yml up -d", file=sys.stderr)
    print("    # wait for the namenode healthcheck to report healthy, then re-run", file=sys.stderr)
    print("=" * 72, file=sys.stderr)
    sys.exit(2)


def detect_mode(namenode: str):
    """Same ladder as push_to_hdfs.py; docker-exec CLI first (host cannot reach :9866)."""
    docker_err = "unknown error"
    try:
        res = subprocess.run([DOCKER, "exec", NAMENODE_CONTAINER, "hdfs", "version"],
                             capture_output=True, timeout=30)
        if res.returncode == 0:
            return ("HDFS_CLI_DOCKER", None)
        docker_err = res.stderr.decode(errors="replace").strip()[:200]
    except FileNotFoundError:
        docker_err = "docker CLI not found on PATH"
    except subprocess.TimeoutExpired:
        docker_err = "docker exec timed out"
    webhdfs_err = "not attempted"
    try:
        from hdfs import InsecureClient
        client = InsecureClient(namenode, user="root", timeout=5)
        client.status("/")
        return ("WebHDFS", client)
    except Exception as exc:
        webhdfs_err = str(exc)
    try:
        res = subprocess.run(["hdfs", "version"], capture_output=True, timeout=15)
        if res.returncode == 0:
            return ("HDFS_CLI", None)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    fail_loudly(f"docker exec {NAMENODE_CONTAINER}: {docker_err}; WebHDFS {namenode}: {webhdfs_err}")


def zone_size(path: str) -> int:
    """Bytes of a file or of every real file under a directory table."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _dirs, names in os.walk(path):
        for n in names:
            p = os.path.join(root, n)
            if not n.startswith((".", "_")):
                total += os.path.getsize(p)
    return total


def zone_rows(path: str):
    """Row count of a parquet file/directory from footer metadata only (no data read)."""
    try:
        import pyarrow.parquet as pq
        if os.path.isfile(path):
            return pq.read_metadata(path).num_rows
        rows = 0
        for root, _dirs, names in os.walk(path):
            for n in names:
                if n.endswith(".parquet") and not n.startswith((".", "_")):
                    rows += pq.read_metadata(os.path.join(root, n)).num_rows
        return rows
    except Exception:
        return None


def _docker_cli() -> list:
    return [DOCKER, "exec", NAMENODE_CONTAINER, "hdfs", "dfs",
            "-D", f"dfs.replication={REPLICATION}", "-D", f"dfs.blocksize={BLOCK_SIZE}"]


def push_cli_docker(local: str, remote: str) -> None:
    """Remove the remote path, then put the local file OR directory table via staging."""
    subprocess.run(_docker_cli() + ["-rm", "-r", "-skipTrash", remote],
                   check=False, capture_output=True)
    subprocess.run(_docker_cli() + ["-mkdir", "-p", os.path.dirname(remote)],
                   check=True, capture_output=True)
    staging = f"/tmp/zonepush_{uuid.uuid4().hex}"
    subprocess.run([DOCKER, "exec", NAMENODE_CONTAINER, "mkdir", "-p", staging],
                   check=True, capture_output=True)
    try:
        subprocess.run([DOCKER, "cp", local, f"{NAMENODE_CONTAINER}:{staging}/"],
                       check=True, capture_output=True)
        subprocess.run(_docker_cli() + ["-put", "-f",
                                        f"{staging}/{os.path.basename(local)}", remote],
                       check=True, capture_output=True)
    finally:
        subprocess.run([DOCKER, "exec", NAMENODE_CONTAINER, "rm", "-rf", staging],
                       capture_output=True)


def push_webhdfs(client, local: str, remote: str) -> None:
    parent = os.path.dirname(remote)
    client.makedirs(parent)
    if os.path.isdir(local):
        for root, _dirs, names in os.walk(local):
            for n in names:
                p = os.path.join(root, n)
                rel = os.path.relpath(p, local).replace(os.sep, "/")
                with open(p, "rb") as f:
                    client.write(f"{remote}/{rel}", f, overwrite=True,
                                 replication=REPLICATION, blocksize=BLOCK_SIZE)
    else:
        with open(local, "rb") as f:
            client.write(remote, f, overwrite=True, replication=REPLICATION, blocksize=BLOCK_SIZE)


def push_cli(local: str, remote: str) -> None:
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-skipTrash", remote],
                   check=False, capture_output=True)
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(remote)],
                   check=True, capture_output=True)
    subprocess.run(["hdfs", "dfs", "-D", f"dfs.replication={REPLICATION}",
                    "-D", f"dfs.blocksize={BLOCK_SIZE}",
                    "-put", "-f", local, remote], check=True, capture_output=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Land the processed and curated zones on HDFS.")
    ap.add_argument("--namenode", default="http://localhost:9870")
    ap.add_argument("--force", action="store_true", help="re-push even if sizes are unchanged")
    ap.add_argument("--fresh", action="store_true", help="ignore the existing zone-sync manifest")
    args = ap.parse_args()

    missing = [loc for loc, _ in ZONE_SOURCES if not os.path.exists(loc)]
    if missing:
        print("[ERROR] zone artifacts missing (run the pipeline stages first):", file=sys.stderr)
        for m in missing:
            print(f"  - {os.path.relpath(m, PROJECT_ROOT)}", file=sys.stderr)
        sys.exit(2)

    mode, client = detect_mode(args.namenode)
    print(f"[mode] {mode}" + (f" ({args.namenode})" if mode == "WebHDFS" else ""))

    manifest = {"files": {}}
    if not args.fresh and os.path.exists(MANIFEST_PATH):
        try:
            with open(MANIFEST_PATH, encoding="utf-8") as f:
                manifest = json.load(f)
        except Exception:
            pass
    manifest.setdefault("files", {})

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    log = open(LOG_PATH, "a", encoding="utf-8")

    total_files = total_bytes = total_blocks = 0
    failures = 0
    log.write(f"=== zone push run started {now_iso()} mode={mode} ===\n")

    for local, remote in ZONE_SOURCES:
        rel = os.path.relpath(local, PROJECT_ROOT).replace(os.sep, "/")
        size = zone_size(local)
        is_dir = os.path.isdir(local)
        kind = "dir-table" if is_dir else "file"
        prev = manifest["files"].get(rel)
        if prev and prev.get("size") == size and not args.force:
            print(f"[skip] {rel} (unchanged, {size:,} B)")
            total_files += prev.get("files", 1)
            total_bytes += size
            total_blocks += math.ceil(size / BLOCK_SIZE)
            continue

        rows = zone_rows(local) if local.endswith(".parquet") or is_dir else None
        try:
            if mode == "WebHDFS":
                push_webhdfs(client, local, remote)
            elif mode == "HDFS_CLI_DOCKER":
                push_cli_docker(local, remote)
            elif mode == "HDFS_CLI":
                push_cli(local, remote)
            else:
                fail_loudly(f"unknown mode {mode}")
            blocks = math.ceil(size / BLOCK_SIZE)
            n_files = sum(len(files) for _r, _d, files in os.walk(local)) if is_dir else 1
            rows_s = f"{rows:,} rows" if rows is not None else "rows n/a"
            log.write(f"{now_iso()} | {mode} | {rel} | {kind} | {rows_s} | "
                      f"size={size:,}B | blocks={blocks} | -> {remote}\n")
            manifest["files"][rel] = {"size": size, "kind": kind, "files": n_files,
                                      "rows": rows, "hdfs": remote, "mode": mode,
                                      "pushed_at": now_iso()}
            total_files += n_files
            total_bytes += size
            total_blocks += blocks
            print(f"[push] {rel} ({kind}, {rows_s}, {size:,} B, {blocks} block(s)) -> {remote}")
        except Exception as exc:
            failures += 1
            log.write(f"{now_iso()} | {mode} | {rel} | ERROR: {exc}\n")
            print(f"[ERROR] {rel}: {exc}")

    manifest["last_sync"] = now_iso()
    manifest["mode"] = mode
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    log.write(f"=== zone push run finished {now_iso()} ===\n")
    log.close()

    print("=== HDFS zone push summary ===")
    print(f"mode         : {mode}")
    print(f"files pushed : {total_files}")
    print(f"total size   : {total_bytes:,} bytes ({total_bytes / 1e6:.1f} MB)")
    print(f"total blocks : {total_blocks} (block size {BLOCK_SIZE // (1024 * 1024)} MB, replication {REPLICATION})")
    print(f"log          : {os.path.relpath(LOG_PATH, PROJECT_ROOT)}")
    if failures:
        print(f"[FAIL] {failures} zone(s) failed to push")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
