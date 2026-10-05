"""
ResellRadar - Task 4: push the immutable raw zone to REAL HDFS (Person 1).

Uploads
    data/raw/mercari/   -> /data/raw/mercari/
    data/raw/generated/ -> /data/raw/generated/run=<ts>/...   (partition kept)

Cluster contract (single-node pseudo-distributed, see docker-compose.yml):
    replication factor = 1, block size = 64 MB (67,108,864 bytes).

Connection modes (tried in order):
    1. WebHDFS   - hdfs library against the NameNode HTTP port (default :9870)
    2. HDFS CLI  - via `docker exec <namenode-container> hdfs ...` (uploads are
                   streamed through `docker cp` into a staging dir), or a local
                   `hdfs` binary if one is on PATH.

There is NO emulation fallback: if no live cluster is reachable the script
exits non-zero with a loud error (FAIL LOUDLY policy).

For every file pushed, logs/ingestion.log records: timestamp, file name,
record count, size in bytes, HDFS target, mode. At the end a summary prints
total files, total bytes, and total HDFS blocks (ceil(size / 64 MB) per file).

Re-pushes skip files already synced (manifest: data/hdfs_sync_manifest.json).
Pass --fresh to start from an empty manifest (the cluster keeps existing files;
point your HDFS paths at a new parent or wipe the cluster for a clean slate).

Usage:
    python scripts/push_to_hdfs.py [--namenode http://localhost:9870] [--force] [--fresh]
"""

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES = [
    (os.path.join(PROJECT_ROOT, "data", "raw", "mercari"), "/data/raw/mercari"),
    (os.path.join(PROJECT_ROOT, "data", "raw", "generated"), "/data/raw/generated"),
    # manual uploads (optional zone): data/raw/uploads/run=<ts>/*.jsonl
    (os.path.join(PROJECT_ROOT, "data", "raw", "uploads"), "/data/raw/uploads"),
]
LOG_PATH = os.path.join(PROJECT_ROOT, "logs", "ingestion.log")
MANIFEST_PATH = os.path.join(PROJECT_ROOT, "data", "hdfs_sync_manifest.json")

NAMENODE_CONTAINER = "resellradar-namenode"
BLOCK_SIZE = 64 * 1024 * 1024
REPLICATION = 1


def _resolve_docker() -> str:
    """docker.exe path: PATH first, then the Docker Desktop install dir."""
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
    print("=" * 72)
    print("FATAL: no live HDFS cluster reachable - push aborted.", file=sys.stderr)
    print(f"Reason: {reason}", file=sys.stderr)
    print("", file=sys.stderr)
    print("Start the real single-node cluster first (no emulation fallback exists):", file=sys.stderr)
    print("    docker compose -f docker-compose.yml up -d", file=sys.stderr)
    print("    # wait for the namenode healthcheck to report healthy", file=sys.stderr)
    print("    docker ps        # namenode (healthy) + datanode (up)", file=sys.stderr)
    print("Then re-run this script.", file=sys.stderr)
    print("=" * 72, file=sys.stderr)
    sys.exit(2)


def count_records(path: str, cached: int | None) -> int:
    """Data-row count: JSONL = lines; TSV = lines - 1 (header). Cached in manifest."""
    if cached is not None:
        return cached
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n - 1 if path.endswith(".tsv") else n


def detect_mode(namenode: str):
    """Return (mode, client). Exits loudly if no real cluster is reachable.

    Docker-exec CLI is tried FIRST: WebHDFS writes from the host are redirected
    by the namenode to the datanode's container hostname, which the host cannot
    resolve - the container CLI has no such problem.
    """
    # 1) hdfs CLI inside the namenode container (no host Hadoop install needed)
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
    # 2) WebHDFS over HTTP (host -> namenode:9870)
    try:
        from hdfs import InsecureClient
        client = InsecureClient(namenode, user="root", timeout=5)
        client.status("/")
        return ("WebHDFS", client)
    except Exception as exc:
        webhdfs_err = str(exc)
    # 3) a real local hdfs binary (full Hadoop install on the host)
    try:
        res = subprocess.run(["hdfs", "version"], capture_output=True, timeout=15)
        if res.returncode == 0:
            return ("HDFS_CLI", None)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    fail_loudly(f"docker exec {NAMENODE_CONTAINER}: {docker_err}; WebHDFS {namenode}: {webhdfs_err}")


def push_webhdfs(client, local: str, remote: str) -> None:
    parent = os.path.dirname(remote)
    client.makedirs(parent)
    with open(local, "rb") as f:
        client.write(remote, f, overwrite=True, replication=REPLICATION, blocksize=BLOCK_SIZE)


def _docker_cli() -> list:
    return [DOCKER, "exec", NAMENODE_CONTAINER, "hdfs", "dfs",
            "-D", f"dfs.replication={REPLICATION}", "-D", f"dfs.blocksize={BLOCK_SIZE}"]


def push_cli_docker(local: str, remote: str) -> None:
    subprocess.run(_docker_cli() + ["-mkdir", "-p", os.path.dirname(remote)],
                   check=True, capture_output=True)
    staging = f"/tmp/push_{uuid.uuid4().hex}"
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


def push_cli(local: str, remote: str) -> None:
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(remote)],
                   check=True, capture_output=True)
    subprocess.run(["hdfs", "dfs", "-D", f"dfs.replication={REPLICATION}",
                    "-D", f"dfs.blocksize={BLOCK_SIZE}",
                    "-put", "-f", local, remote], check=True, capture_output=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namenode", default="http://localhost:9870")
    ap.add_argument("--force", action="store_true", help="re-upload even if already synced")
    ap.add_argument("--fresh", action="store_true", help="ignore the existing sync manifest")
    args = ap.parse_args()

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
    log.write(f"=== push run started {now_iso()} mode={mode} ===\n")

    for local_dir, hdfs_dir in SOURCES:
        if not os.path.isdir(local_dir):
            print(f"[skip] {os.path.relpath(local_dir, PROJECT_ROOT)} missing")
            continue
        files = sorted(
            os.path.join(root, f)
            for root, _dirs, names in os.walk(local_dir) for f in names
            if not f.startswith(".")
        )
        if not files:
            if os.path.basename(local_dir) == "uploads":
                continue  # the uploads zone is optional - an empty one is fine
            print(f"[ERROR] {os.path.relpath(local_dir, PROJECT_ROOT)} exists but is empty - "
                  f"raw zone must not be empty; refusing to continue")
            failures += 1
            continue
        print(f"[scan] {os.path.relpath(local_dir, PROJECT_ROOT)}: {len(files)} files")
        for fpath in files:
            rel = os.path.relpath(fpath, PROJECT_ROOT)
            size = os.path.getsize(fpath)
            prev = manifest["files"].get(rel.replace(os.sep, "/"))
            if prev and prev.get("size") == size and not args.force:
                total_files += 1
                total_bytes += size
                total_blocks += math.ceil(size / BLOCK_SIZE)
                continue  # already in HDFS, counted in totals

            recs = count_records(fpath, (prev or {}).get("records"))
            # preserve any sub-partition structure (e.g. run=<ts>/) inside the HDFS dir
            rel_to_src = os.path.relpath(fpath, local_dir).replace(os.sep, "/")
            remote = f"{hdfs_dir}/{rel_to_src}"
            try:
                if mode == "WebHDFS":
                    push_webhdfs(client, fpath, remote)
                elif mode == "HDFS_CLI_DOCKER":
                    push_cli_docker(fpath, remote)
                elif mode == "HDFS_CLI":
                    push_cli(fpath, remote)
                else:
                    fail_loudly(f"unknown mode {mode}")
                blocks = math.ceil(size / BLOCK_SIZE)
                log.write(f"{now_iso()} | {mode} | {rel} | records={recs:,} | "
                          f"size={size:,}B | blocks={blocks} | -> {remote}\n")
                manifest["files"][rel.replace(os.sep, "/")] = {"size": size, "records": recs,
                                                               "hdfs": remote, "mode": mode,
                                                               "pushed_at": now_iso()}
                total_files += 1
                total_bytes += size
                total_blocks += blocks
                print(f"[push] {rel} ({recs:,} records, {size:,} B, {blocks} block(s))")
            except Exception as exc:
                failures += 1
                log.write(f"{now_iso()} | {mode} | {rel} | ERROR: {exc}\n")
                print(f"[ERROR] {rel}: {exc}")

    manifest["last_sync"] = now_iso()
    manifest["mode"] = mode
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    log.write(f"=== push run finished {now_iso()} ===\n")
    log.close()

    print("=== HDFS push summary ===")
    print(f"mode         : {mode}")
    print(f"files        : {total_files}")
    print(f"total size   : {total_bytes:,} bytes ({total_bytes / 1e6:.1f} MB)")
    print(f"total blocks : {total_blocks} (block size {BLOCK_SIZE // (1024 * 1024)} MB, replication {REPLICATION})")
    print(f"log          : {os.path.relpath(LOG_PATH, PROJECT_ROOT)}")
    if failures:
        print(f"[FAIL] {failures} file(s) failed to push")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
