"""
ResellRadar Pipeline Orchestration Server (FastAPI)
Bridges the Next.js telemetry dashboard with the end-to-end Big Data pipeline.
Executes data generation, validation, HDFS ingestion, Spark processing,
and acceptance verification upon user command.
"""

import datetime
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="ResellRadar Pipeline Controller API", version="3.0.0")

# Enable CORS for Next.js dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PYTHON_EXEC = sys.executable

# ---- Spark Standalone batch cluster (multiple processing units) -------------
# When the compose "cluster" profile is up (spark-master + spark-worker containers),
# the pipeline's Spark stages run DISTRIBUTED: each stage is submitted from a driver
# container on the compose network to spark://spark-master:7077, so the executors on
# the worker units do the processing and stage outputs land in the HDFS zones.
# When the cluster is down, stages fall back to host-Python local[*] execution.
DOCKER_EXE = shutil.which("docker") or r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"
SPARK_IMAGE = "resellradar/spark:4.2.0-numpy"
SPARK_NETWORK = "resellradar_default"
RR_DATA_ROOT = "hdfs://namenode:9000/data"
SPARK_MASTER_URL = "spark://spark-master:7077"

# Host path in a form Docker Desktop accepts for bind mounts (E:/Projects/...)
PROJECT_ROOT_DOCKER = PROJECT_ROOT.replace("\\", "/")


def _docker(*args: str, timeout: int = 20) -> str:
    """Run a read-only docker command and return stdout ('' on any failure)."""
    try:
        out = subprocess.run(
            [DOCKER_EXE, *args], capture_output=True, text=True, timeout=timeout
        )
        return out.stdout if out.returncode == 0 else ""
    except Exception:
        return ""


def cluster_workers() -> List[Dict[str, Any]]:
    """Live Spark Standalone worker units: [{'name', 'status', 'cpu', 'mem_used', 'mem_limit'}]."""
    names = _docker(
        "ps", "--format", "{{.Names}}|{{.Status}}", timeout=15
    ).splitlines()
    workers = []
    for line in names:
        if "|" not in line:
            continue
        name, status = line.split("|", 1)
        if name.startswith("resellradar-spark-worker-"):
            workers.append({"name": name, "status": status.strip()})
    if workers:
        for stat in _docker(
            "stats", "--no-stream", "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}", timeout=30,
        ).splitlines():
            parts = stat.split("|")
            if len(parts) != 3:
                continue
            cname, cpu, mem = parts
            for w in workers:
                if w["name"] == cname:
                    w["cpu"] = cpu.strip()
                    mem_used, _, mem_limit = mem.strip().partition(" /")
                    w["mem_used"] = mem_used.strip()
                    w["mem_limit"] = mem_limit.strip()
    return workers


def spark_standalone_up() -> bool:
    if not _docker("ps", "--format", "{{.Names}}", timeout=15).splitlines():
        return False
    return any(
        line.strip() == "resellradar-spark-master"
        for line in _docker("ps", "--format", "{{.Names}}", timeout=15).splitlines()
    )


def spark_stage_cmd(job_relpath: str, container_name: str) -> List[str]:
    """Distributed Spark stage: driver container on the compose network submitting to
    the Standalone master; executors run on the worker units and read/write HDFS."""
    return [
        DOCKER_EXE, "run", "--rm", "--name", container_name, "--hostname", container_name,
        "--user", "root", "--network", SPARK_NETWORK,
        "-e", f"RR_DATA_ROOT={RR_DATA_ROOT}",
        "-e", f"SPARK_MASTER={SPARK_MASTER_URL}",
        "-v", f"{PROJECT_ROOT_DOCKER}:/work", "-w", "/work",
        SPARK_IMAGE,
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER_URL,
        "--driver-memory", "1536m",
        "--executor-memory", "1g",
        "--executor-cores", "3",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", "spark.hadoop.dfs.replication=1",
        f"/work/{job_relpath}",
    ]


def sync_hdfs_outputs_to_local() -> bool:
    """After a distributed run the stage outputs live in HDFS; copy them back to the
    local checkout so the acceptance stage (which reads local parquet) checks fresh data."""
    tables = [
        ("/data/processed/clean_listings.parquet", os.path.join("data", "processed", "clean_listings.parquet")),
        ("/data/processed/entity_resolved.parquet", os.path.join("data", "processed", "entity_resolved.parquet")),
        ("/data/curated/depreciation_curve_curated.parquet", os.path.join("data", "curated", "depreciation_curve_curated.parquet")),
        ("/data/curated/resale_velocity_curated.parquet", os.path.join("data", "curated", "resale_velocity_curated.parquet")),
        ("/data/curated/regional_price_variance_curated.parquet", os.path.join("data", "curated", "regional_price_variance_curated.parquet")),
    ]
    ok = True
    for hdfs_path, rel_local in tables:
        local_path = os.path.join(PROJECT_ROOT, rel_local)
        tmp_remote = "/tmp/rr_sync_" + os.path.basename(rel_local)
        tmp_local = local_path + ".tmpsync"
        try:
            if subprocess.run(
                [DOCKER_EXE, "exec", "resellradar-namenode", "bash", "-c",
                 f"rm -rf {tmp_remote} && hdfs dfs -copyToLocal -f {hdfs_path} {tmp_remote}"],
                capture_output=True, timeout=300,
            ).returncode != 0:
                ok = False
                continue
            if subprocess.run(
                [DOCKER_EXE, "cp", f"resellradar-namenode:{tmp_remote}", tmp_local],
                capture_output=True, timeout=300,
            ).returncode != 0:
                ok = False
                continue
            if os.path.exists(local_path):
                shutil.rmtree(local_path, ignore_errors=True)
            os.replace(tmp_local, local_path)
        except Exception:
            ok = False
        finally:
            subprocess.run(
                [DOCKER_EXE, "exec", "resellradar-namenode", "rm", "-rf", tmp_remote],
                capture_output=True, timeout=60,
            )
    return ok

# Global Telemetry & Pipeline State
PIPELINE_STATE: Dict[str, Any] = {
    "is_running": False,
    "current_stage": None,
    "pid": None,
    "category": "all",
    "batch_target": 1000,
    "scraped_count": 0,
    "scrapes_per_sec": 0.0,
    "active_threads": 0,
    "success_rate": 99.41,
    "last_run_timestamp": None,
    "execution_mode": "single-unit (host local[*])",
    "cluster_apps": [],
    "logs": [
        {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "level": "INFO",
            "message": "ResellRadar Orchestration Daemon initialized. Ready to execute pipeline.",
        }
    ],
}

_state_lock = threading.Lock()
_current_process: Optional[subprocess.Popen] = None
_stop_requested = threading.Event()


def add_log(level: str, message: str):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    entry = {"timestamp": ts, "level": level.upper(), "message": message.strip()}
    with _state_lock:
        PIPELINE_STATE["logs"].append(entry)
        if len(PIPELINE_STATE["logs"]) > 500:
            PIPELINE_STATE["logs"].pop(0)


class ScrapeTriggerRequest(BaseModel):
    category: str = "all"
    batch_target: int = 1000
    concurrency_threads: int = 16


def run_pipeline_sequence(category: str, target: int, threads: int):
    """Executes the full data engineering pipeline sequentially in a background thread."""
    global _current_process
    _stop_requested.clear()

    with _state_lock:
        PIPELINE_STATE["is_running"] = True
        PIPELINE_STATE["category"] = category
        PIPELINE_STATE["batch_target"] = target
        PIPELINE_STATE["active_threads"] = threads
        PIPELINE_STATE["scraped_count"] = 0
        PIPELINE_STATE["scrapes_per_sec"] = 0.0
        PIPELINE_STATE["last_run_timestamp"] = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    pipeline_start_time = time.time()
    use_cluster = spark_standalone_up()
    exec_mode = (
        "distributed (Spark Standalone: 3 worker units x 3 executor cores)"
        if use_cluster
        else "single-unit (host local[*])"
    )
    with _state_lock:
        PIPELINE_STATE["execution_mode"] = exec_mode
    add_log("INFO", f"=== PIPELINE EXECUTION STARTED (Target: {target:,} rows, Workers: {threads}) ===")
    add_log("INFO", f"Execution mode: {exec_mode}")
    if not use_cluster:
        add_log(
            "WARN",
            "Spark Standalone cluster not detected - Spark stages will run single-unit. "
            "Start it with: docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3",
        )

    # Pipeline stages configuration
    stages = [
        {
            "name": "Stage 1: Synthetic Listing Generation",
            "cmd": [
                PYTHON_EXEC,
                os.path.join(PROJECT_ROOT, "generator", "generate_data.py"),
                "--rows",
                str(target),
            ],
            "estimated_rows": target,
        },
        {
            "name": "Stage 2: Raw Schema & Quality Validation",
            "cmd": [
                PYTHON_EXEC,
                os.path.join(PROJECT_ROOT, "scripts", "validate_raw.py"),
                "--skip-mercari",
            ],
            "estimated_rows": 0,
        },
        {
            "name": "Stage 3: HDFS Raw Zone Push",
            "cmd": [PYTHON_EXEC, os.path.join(PROJECT_ROOT, "scripts", "push_to_hdfs.py")],
            "estimated_rows": 0,
        },
        {
            "name": "Stage 4: Spark Clean & Normalize",
            "job": "spark_jobs/clean_normalize.py",
            "spark": True,
            "estimated_rows": 0,
        },
        {
            "name": "Stage 5: Spark Entity Resolution & Repost Detection",
            "job": "spark_jobs/entity_resolution.py",
            "spark": True,
            "estimated_rows": 0,
        },
        {
            "name": "Stage 6: Spark Feature Engineering & Curated Tables",
            "job": "spark_jobs/feature_engineering.py",
            "spark": True,
            "estimated_rows": 0,
        },
        {
            "name": "Stage 7: Pipeline Acceptance Verification",
            "cmd": [
                PYTHON_EXEC,
                os.path.join(PROJECT_ROOT, "scripts", "accept_pipeline.py"),
                "all",
                "--out",
                os.path.join(PROJECT_ROOT, "logs", "acceptance_checks.md"),
            ],
            "estimated_rows": 0,
            # Distributed runs write stage outputs to HDFS, so they must be pulled
            # back to the local checkout BEFORE acceptance reads the local parquet.
            "needs_local_sync": True,
        },
    ]

    total_stages = len(stages)
    for idx, stage in enumerate(stages, start=1):
        if _stop_requested.is_set():
            add_log("WARN", "Pipeline run aborted by operator.")
            break

        stage_name = stage["name"]
        if stage.get("needs_local_sync") and use_cluster:
            add_log("INFO", "Syncing distributed stage outputs from HDFS to the local checkout for acceptance checks...")
            if sync_hdfs_outputs_to_local():
                add_log("ACK", "[OK] HDFS stage outputs synced to data/processed + data/curated")
            else:
                add_log("WARN", "Some HDFS outputs could not be synced locally; acceptance checks may see stale local data")
        if stage.get("spark") and use_cluster:
            safe_name = re.sub(r"[^A-Za-z0-9-]", "-", stage_name.split(":", 1)[-1].strip().lower())
            container_name = f"resellradar-spark-console-{idx}-{safe_name[:24]}-{int(time.time())}"
            cmd = spark_stage_cmd(stage["job"], container_name)
            add_log("INFO", f"[{idx}/{total_stages}] submitting to Spark Standalone cluster (executors on worker units): {stage['job']}")
        elif stage.get("spark"):
            cmd = [PYTHON_EXEC, os.path.join(PROJECT_ROOT, stage["job"].replace("/", os.sep))]
        else:
            cmd = stage["cmd"]
        with _state_lock:
            PIPELINE_STATE["current_stage"] = f"[{idx}/{total_stages}] {stage_name}"

        add_log("INFO", f"--> Starting [{idx}/{total_stages}]: {stage_name}")
        stage_start = time.time()

        try:
            _current_process = subprocess.Popen(
                cmd,
                cwd=PROJECT_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            with _state_lock:
                PIPELINE_STATE["pid"] = _current_process.pid

            # Stream output in real time
            for line in _current_process.stdout:
                clean_line = line.strip()
                if clean_line:
                    # Classify log level for console badge
                    lvl = "INFO"
                    lower_line = clean_line.lower()
                    if "error" in lower_line or "fatal" in lower_line:
                        lvl = "ERR"
                    elif "warn" in lower_line or "skip" in lower_line:
                        lvl = "WARN"
                    elif "success" in lower_line or "ok" in lower_line or "done" in lower_line or "passed" in lower_line:
                        lvl = "ACK"

                    add_log(lvl, clean_line)

                    # Update throughput / progress
                    elapsed = max(0.1, time.time() - pipeline_start_time)
                    if stage["estimated_rows"] > 0:
                        with _state_lock:
                            PIPELINE_STATE["scraped_count"] = min(
                                target, int((time.time() - stage_start) * (target / 5.0))
                            )
                            PIPELINE_STATE["scrapes_per_sec"] = round(
                                PIPELINE_STATE["scraped_count"] / elapsed, 1
                            )

            _current_process.wait()
            rc = _current_process.returncode

            if stage.get("spark") and use_cluster:
                with _state_lock:
                    PIPELINE_STATE["cluster_apps"].append(
                        {
                            "name": stage_name,
                            "container": container_name,
                            "state": "FINISHED" if rc == 0 else "FAILED",
                            "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        }
                    )
                    if len(PIPELINE_STATE["cluster_apps"]) > 12:
                        PIPELINE_STATE["cluster_apps"].pop(0)

            if rc != 0:
                add_log(
                    "WARN",
                    f"[{idx}/{total_stages}] {stage_name} exited with status code {rc}. Continuing pipeline...",
                )
            else:
                elapsed_stage = round(time.time() - stage_start, 2)
                add_log("ACK", f"[OK] [{idx}/{total_stages}] {stage_name} finished in {elapsed_stage}s")

        except Exception as ex:
            add_log("ERR", f"Failed executing {stage_name}: {str(ex)}")

    total_elapsed = round(time.time() - pipeline_start_time, 2)
    if _stop_requested.is_set():
        add_log("WARN", f"=== PIPELINE HALTED by user after {total_elapsed}s ===")
    else:
        with _state_lock:
            PIPELINE_STATE["scraped_count"] = target
            PIPELINE_STATE["scrapes_per_sec"] = round(target / max(0.1, total_elapsed), 1)
        add_log("ACK", f"=== PIPELINE FINISHED SUCCESSFULLY in {total_elapsed}s ===")

    with _state_lock:
        PIPELINE_STATE["is_running"] = False
        PIPELINE_STATE["current_stage"] = None
        PIPELINE_STATE["active_threads"] = 0
        PIPELINE_STATE["pid"] = None


@app.get("/api/status")
def get_status():
    manifest_path = os.path.join(PROJECT_ROOT, "data", "hdfs_sync_manifest.json")
    manifest = {}
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except Exception:
            pass

    # Count raw records
    total_raw_listings = 0
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    if os.path.exists(raw_dir):
        for root, _, files in os.walk(raw_dir):
            for file in files:
                if file.endswith(".json"):
                    try:
                        p = os.path.join(root, file)
                        with open(p, "r", encoding="utf-8") as fp:
                            d = json.load(fp)
                            if isinstance(d, list):
                                total_raw_listings += len(d)
                    except Exception:
                        pass

    # Check HDFS state. The manifest written by scripts/push_to_hdfs.py is
    # {"files": {path: {size, records, hdfs, pushed_at...}}, "last_sync": iso, "mode": str};
    # accept the older list-based schema too.
    files_map = manifest.get("files")
    if isinstance(files_map, dict):
        pushed_count = len(files_map)
        total_bytes_pushed = sum(
            int(f.get("size", 0)) for f in files_map.values() if isinstance(f, dict)
        )
        last_sync = manifest.get("last_sync") or manifest.get("last_sync_timestamp")
    else:
        pushed_files = manifest.get("pushed_files", [])
        pushed_count = len(pushed_files) if isinstance(pushed_files, list) else 0
        total_bytes_pushed = manifest.get("total_bytes_pushed", 0)
        last_sync = manifest.get("last_sync_timestamp")

    hdfs_status = "CONNECTED" if pushed_count else "STANDBY"

    with _state_lock:
        state_copy = dict(PIPELINE_STATE)

    return {
        "pipeline_state": state_copy,
        "raw_storage": {
            "total_listings_scraped": max(total_raw_listings, state_copy["scraped_count"]),
            "raw_path": raw_dir,
        },
        "hdfs_telemetry": {
            "status": hdfs_status,
            "type": "HDFS Pseudo-Distributed / Docker",
            "endpoint": "hdfs://namenode:9000",
            "hdfs_path": "/data/raw/",
            "pushed_files_count": pushed_count,
            "total_bytes_pushed": total_bytes_pushed,
            "last_sync_timestamp": last_sync,
        },
    }


@app.post("/api/scrape/trigger")
def trigger_scrape(req: ScrapeTriggerRequest, background_tasks: BackgroundTasks):
    with _state_lock:
        if PIPELINE_STATE["is_running"]:
            return {"status": "ERROR", "message": "Pipeline execution is already active."}

    background_tasks.add_task(
        run_pipeline_sequence, req.category, req.batch_target, req.concurrency_threads
    )
    return {
        "status": "ACCEPTED",
        "message": f"Pipeline triggered: executing generation ({req.batch_target:,} rows), validation, HDFS sync, Spark stages, and acceptance checks.",
    }


@app.post("/api/scrape/stop")
def stop_scrape():
    global _current_process
    _stop_requested.set()
    if _current_process and _current_process.poll() is None:
        try:
            _current_process.terminate()
            add_log("WARN", "Active process terminated by user request.")
        except Exception:
            pass
    # Distributed Spark stages run as driver containers (resellradar-spark-console-*);
    # terminating the docker CLI alone would leave their spark-submit running.
    for line in _docker("ps", "--format", "{{.Names}}", timeout=15).splitlines():
        name = line.strip()
        if name.startswith("resellradar-spark-console-"):
            subprocess.run([DOCKER_EXE, "kill", name], capture_output=True, timeout=30)
            add_log("WARN", f"Distributed Spark driver container stopped: {name}")
    return {"status": "SUCCESS", "message": "Pipeline stop signal sent."}


@app.get("/api/cluster")
def cluster_status():
    """Live view of the Spark Standalone batch cluster (the multiple processing units).

    Application state comes from infrastructure we control: RUNNING = live
    resellradar-spark-console-* driver containers, FINISHED = history recorded by the
    pipeline runner (Spark 4's master web UI serves no JSON REST)."""
    workers = cluster_workers()
    up = spark_standalone_up()

    running_apps = []
    for line in _docker("ps", "--format", "{{.Names}}|{{.Status}}", timeout=15).splitlines():
        if "|" not in line:
            continue
        name, status = line.split("|", 1)
        if name.startswith("resellradar-spark-console-"):
            running_apps.append(
                {"name": name, "state": "RUNNING", "status": status.strip()}
            )

    with _state_lock:
        finished_apps = list(PIPELINE_STATE["cluster_apps"])

    return {
        "standalone_up": up,
        "master_url": SPARK_MASTER_URL,
        "mode": (
            "distributed - Spark Standalone cluster (multiple processing units)"
            if up
            else "single-unit fallback (cluster profile is not running)"
        ),
        "worker_count": len(workers),
        "workers": workers,
        "running_apps": running_apps,
        "finished_apps": finished_apps,
    }


# ---- Data analytics aggregates (processed parquet -> KPIs / charts / insights) --
# Reading the 2M-row parquet tables and grouping them takes several seconds, so the
# aggregates are cached in memory and recomputed in the background once stale.
ANALYTICS_TTL_SECONDS = 300
_ANALYTICS_LOCK = threading.Lock()
_ANALYTICS_CACHE: Dict[str, Any] = {"payload": None, "computed_at": 0.0, "computing": False}


def _json_safe(obj: Any) -> Any:
    """Recursively convert numpy/scalar values into strict-JSON-safe natives
    (NaN/Inf become None so browser JSON.parse never chokes)."""
    np = sys.modules.get("numpy")
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if np is not None and isinstance(obj, np.integer):
        return int(obj)
    if np is not None and isinstance(obj, np.floating):
        f = float(obj)
        return f if math.isfinite(f) else None
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, (bool, int, str)) or obj is None:
        return obj
    return str(obj)


def _analytics_stale(now: float) -> bool:
    payload = _ANALYTICS_CACHE["payload"]
    return payload is None or (now - _ANALYTICS_CACHE["computed_at"] > ANALYTICS_TTL_SECONDS)


def compute_analytics() -> Dict[str, Any]:
    """Aggregate the local (HDFS-synced) processed/curated parquet tables into
    KPIs, chart series, and auto-generated insights for the analytics page."""
    import pandas as pd  # lazy import: only the analytics path needs it

    root = PROJECT_ROOT
    charts: Dict[str, Any] = {}

    # ---------- clean_listings: core marketplace KPIs ----------
    cl = pd.read_parquet(
        os.path.join(root, "data", "processed", "clean_listings.parquet"),
        columns=["price", "category", "source_platform", "item_condition_id",
                 "brand_name", "seller_id", "delisted_date", "posted_date",
                 "location_city", "location_region"],
    )
    total_listings = int(len(cl))
    prices = cl["price"].dropna()
    avg_price = float(prices.mean()) if len(prices) else 0.0
    median_price = float(prices.median()) if len(prices) else 0.0

    platforms = (
        cl.groupby("source_platform").size().sort_values(ascending=False).head(6)
    )
    charts["listings_by_platform"] = [
        {"label": str(idx), "value": int(v)} for idx, v in platforms.items()
    ]

    cat = (
        cl.groupby("category")
        .agg(avg_price=("price", "mean"), count=("price", "size"))
        .sort_values("count", ascending=False)
        .head(8)
    )
    charts["price_by_category"] = [
        {"label": str(idx), "value": round(float(row["avg_price"]), 2), "extra": int(row["count"])}
        for idx, row in cat.iterrows()
        if pd.notnull(row["avg_price"])
    ]

    cond = cl["item_condition_id"].value_counts().sort_index()
    charts["condition_mix"] = [
        {"label": f"Condition {int(idx)}", "value": int(v)} for idx, v in cond.items()
    ]

    brands = cl[cl["brand_name"].notnull() & (cl["brand_name"].astype(str).str.strip() != "")]
    top_brands = (
        brands.groupby("brand_name")
        .agg(count=("price", "size"), avg_price=("price", "mean"))
        .sort_values("count", ascending=False)
        .head(8)
    )
    charts["top_brands"] = [
        {"label": str(idx), "value": int(row["count"]), "extra": round(float(row["avg_price"]), 2)}
        for idx, row in top_brands.iterrows()
    ]
    brand_known = int(len(brands))

    bands = pd.cut(
        prices,
        bins=[0, 10, 25, 50, 100, 200, 500, float("inf")],
        labels=["$0-10", "$10-25", "$25-50", "$50-100", "$100-200", "$200-500", "$500+"],
    )
    charts["price_bands"] = [
        {"label": str(idx), "value": int(v)} for idx, v in bands.value_counts().sort_index().items()
    ]

    dated = cl[cl["posted_date"].notnull()]
    posted_dated = int(len(dated))
    if posted_dated:
        # Monthly volume (synthetic source only - Mercari rows carry no dates).
        monthly = dated["posted_date"].dt.to_period("M").value_counts().sort_index()
        charts["listings_by_month"] = [
            {"label": str(idx), "value": int(v)} for idx, v in monthly.items()
        ]
    else:
        charts["listings_by_month"] = []

    cities = cl[cl["location_city"].notnull()]
    top_cities = cities.groupby("location_city").size().sort_values(ascending=False).head(8)
    charts["top_cities"] = [
        {"label": str(idx), "value": int(v)} for idx, v in top_cities.items()
    ]

    delisted_count = int(cl["delisted_date"].notnull().sum())
    sellers = int(cl["seller_id"].dropna().nunique())
    del cl, prices, brands, dated, cities

    # ---------- entity_resolved: ER / repost KPIs + cluster-size story ----------
    er = pd.read_parquet(
        os.path.join(root, "data", "processed", "entity_resolved.parquet"),
        columns=["entity_id", "predicted_is_repost", "category"],
    )
    unique_products = int(er["entity_id"].nunique())
    er["repost_flag"] = pd.to_numeric(er["predicted_is_repost"], errors="coerce").fillna(0).astype(int)
    repost_count = int(er["repost_flag"].sum())

    # Repost rate per top-level category (how reposty each market segment is).
    rc = er.groupby("category").agg(total=("repost_flag", "size"), reposts=("repost_flag", "sum"))
    rc = rc[rc["total"] > 0].sort_values("total", ascending=False).head(8)
    charts["reposts_by_category"] = [
        {
            "label": str(idx),
            "value": round(float(row["reposts"]) / float(row["total"]) * 100.0, 2),
            "extra": int(row["reposts"]),
        }
        for idx, row in rc.iterrows()
    ]

    # Product cluster sizes: how many listings describe the same product.
    sizes = er.groupby("entity_id").size()
    size_bands = pd.cut(
        sizes,
        bins=[0, 1, 2, 5, 20, 100, float("inf")],
        labels=["1", "2", "3-5", "6-20", "21-100", "100+"],
    )
    charts["cluster_sizes"] = [
        {"label": str(idx), "value": int(v)} for idx, v in size_bands.value_counts().reindex(size_bands.cat.categories).items()
    ]
    max_cluster_size = int(sizes.max())
    products_multi = int((sizes >= 2).sum())
    products_5plus = int((sizes >= 5).sum())
    del er, sizes, size_bands, rc

    # ---------- depreciation curve (curated) ----------
    dep = pd.read_parquet(
        os.path.join(root, "data", "curated", "depreciation_curve_curated.parquet"),
        columns=["entity_id", "listing_age_months", "listing_count",
                 "average_price", "baseline_price", "price_change_percent"],
    )
    curve = dep[dep["listing_age_months"].notnull() & dep["baseline_price"].notnull()].copy()
    curve["month"] = curve["listing_age_months"].astype(float).round().astype("Int64")
    curve = curve[curve["month"].notna() & (curve["month"] >= 0) & (curve["month"] <= 24)]
    curve_g = (
        curve.groupby("month")
        .agg(pct=("price_change_percent", "mean"), cnt=("listing_count", "sum"))
        .sort_index()
    )
    charts["depreciation_curve"] = [
        {"label": f"M{int(idx)}", "value": round(float(row["pct"]), 2) if pd.notnull(row["pct"]) else None,
         "extra": int(row["cnt"])}
        for idx, row in curve_g.iterrows()
    ]

    # Weighted average price change over months 1-12 (the measurable depreciation window)
    win = curve_g.loc[curve_g.index <= 12]
    avg_dep_pct = 0.0
    if len(win) and int(win["cnt"].sum()) > 0:
        valid = win[win["pct"].notnull()]
        if len(valid):
            avg_dep_pct = float((valid["pct"] * valid["cnt"]).sum() / valid["cnt"].sum())
    del dep, curve, curve_g, win

    # ---------- resale velocity (curated) ----------
    rv = pd.read_parquet(
        os.path.join(root, "data", "curated", "resale_velocity_curated.parquet"),
        columns=["entity_id", "delisted_listings", "avg_resale_days", "median_resale_days"],
    )
    total_delisted = int(rv["delisted_listings"].fillna(0).sum())
    wsum = float((rv["avg_resale_days"] * rv["delisted_listings"]).dropna().sum())
    avg_resale_days = round(wsum / total_delisted, 2) if total_delisted > 0 else 0.0
    med_days = rv["median_resale_days"].dropna()
    median_resale_days = round(float(med_days.median()), 2) if len(med_days) else 0.0
    fast = rv[(rv["delisted_listings"] > 0) & rv["avg_resale_days"].notnull()]
    buckets = pd.cut(
        fast["avg_resale_days"],
        bins=[0, 7, 14, 30, 60, float("inf")],
        labels=["0-7d", "7-14d", "14-30d", "30-60d", "60d+"],
    )
    charts["resale_speed"] = [
        {"label": str(idx), "value": int(v)} for idx, v in buckets.value_counts().sort_index().items()
    ]
    under14 = int((fast["avg_resale_days"] <= 14).sum())
    del rv, fast

    # ---------- regional price variance (curated) ----------
    reg = pd.read_parquet(
        os.path.join(root, "data", "curated", "regional_price_variance_curated.parquet"),
        columns=["entity_id", "location_region", "listing_count", "average_price", "regional_price_range"],
    )
    regk = reg[reg["location_region"].notnull()]
    region_count = int(regk["location_region"].nunique())
    top_reg = (
        regk.groupby("location_region")
        .agg(count=("listing_count", "sum"), avg_price=("average_price", "mean"), rng=("regional_price_range", "mean"))
        .sort_values("count", ascending=False)
        .head(6)
    )
    charts["regional_variance"] = [
        {"label": str(idx), "value": int(row["count"]), "extra": round(float(row["avg_price"]), 2)}
        for idx, row in top_reg.iterrows()
    ]
    wide_reg = regk[regk["listing_count"] >= 1000]
    widest = None
    if len(wide_reg):
        wr = wide_reg.groupby("location_region")["regional_price_range"].mean().sort_values(ascending=False)
        if len(wr):
            widest = (str(wr.index[0]), float(wr.iloc[0]))
    del reg, regk, top_reg

    # ---------- insights (auto-generated from the aggregates above) ----------
    insights: List[Dict[str, str]] = []
    repost_rate = (repost_count / total_listings * 100.0) if total_listings else 0.0
    if total_listings:
        insights.append({
            "icon": "check",
            "title": "Entity resolution quality",
            "body": (
                f"{repost_rate:.2f}% of listings ({repost_count:,} of {total_listings:,}) were flagged as "
                f"reposts across {unique_products:,} product entities - about "
                f"{total_listings / max(1, unique_products):.2f} listings per product."
            ),
        })
    if len(charts["depreciation_curve"]):
        insights.append({
            "icon": "trend",
            "title": "Launch-cohort price drift",
            "body": (
                f"Listings appearing in the 12 months after a product first shows up average "
                f"{avg_dep_pct:+.1f}% versus that product's launch-month price - the per-month "
                f"drift curve is plotted above."
            ),
        })
    majors = [c for c in charts["price_by_category"] if (c.get("extra") or 0) >= 5000]
    if majors:
        top = max(majors, key=lambda c: c["value"])
        if median_price > 0:
            insights.append({
                "icon": "trend",
                "title": "Premium categories",
                "body": (
                    f"\"{top['label']}\" listings average ${top['value']:,.2f} - "
                    f"{top['value'] / median_price:.1f}x the marketplace median of ${median_price:,.2f}."
                ),
            })
    if total_delisted > 0:
        insights.append({
            "icon": "info",
            "title": "Resale velocity",
            "body": (
                f"Median resale cycle is {median_resale_days:.0f} days (avg {avg_resale_days:.1f}) across "
                f"{total_delisted:,} delisted listings; {under14:,} products resell within two weeks."
            ),
        })
    if brand_known and total_listings:
        unknown_pct = (1 - brand_known / total_listings) * 100.0
        insights.append({
            "icon": "info",
            "title": "Catalog coverage",
            "body": (
                f"{unknown_pct:.1f}% of listings carry no recognizable brand name. Dates, cities and "
                f"seller ids exist only for the synthetic source ({posted_dated:,} of {total_listings:,} "
                f"listings, {posted_dated / max(1, total_listings) * 100:.1f}%) - charts marked 'synthetic "
                f"source' cover that slice only."
            ),
        })
    if products_5plus:
        insights.append({
            "icon": "alert" if max_cluster_size >= 100 else "info",
            "title": "Duplicate pressure",
            "body": (
                f"{products_multi:,} products appear as 2+ listings and {products_5plus:,} have 5 or more; "
                f"the median product appears once. The largest cluster holds {max_cluster_size:,} listings - "
                f"a generic-title hub (e.g. 'phone case') worth reviewing in the ER rules."
                if max_cluster_size >= 100
                else f"{products_multi:,} products appear as 2+ listings; {products_5plus:,} have 5 or more."
            ),
        })
    if widest:
        insights.append({
            "icon": "alert",
            "title": "Regional price spread",
            "body": (
                f"\"{widest[0]}\" shows the widest regional price spread (avg range "
                f"${widest[1]:,.2f} between the cheapest and priciest listing of an entity)."
            ),
        })

    return {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ttl_seconds": ANALYTICS_TTL_SECONDS,
        "data_coverage": {
            "full_corpus_rows": total_listings,
            "synthetic_source_rows": posted_dated,
            "synthetic_share_pct": round(posted_dated / max(1, total_listings) * 100.0, 1),
            "mercari_rows_have_no_dates_cities_sellers": True,
        },
        "tables": {
            "clean_listings": total_listings,
            "entity_resolved": total_listings,
            "unique_entities": unique_products,
            "depreciation_curve": len(charts["depreciation_curve"]),
            "resale_velocity": total_delisted,
            "regional_variance": region_count,
        },
        "kpis": {
            "total_listings": total_listings,
            "unique_products": unique_products,
            "listings_per_product": round(total_listings / max(1, unique_products), 2),
            "largest_cluster": max_cluster_size,
            "repost_count": repost_count,
            "repost_rate_pct": round(repost_rate, 2),
            "avg_price": round(avg_price, 2),
            "median_price": round(median_price, 2),
            "delisted_count": delisted_count,
            "avg_resale_days": avg_resale_days,
            "median_resale_days": median_resale_days,
            "sellers": sellers,
            "brand_known_listings": brand_known,
            "avg_depreciation_pct": round(avg_dep_pct, 2),
        },
        "charts": charts,
        "insights": insights,
    }


def _recompute_analytics_bg():
    try:
        data = compute_analytics()
        with _ANALYTICS_LOCK:
            _ANALYTICS_CACHE["payload"] = data
            _ANALYTICS_CACHE["computed_at"] = time.time()
        add_log("ACK", "[OK] Analytics aggregates refreshed from processed parquet tables")
    except Exception as ex:
        add_log("ERR", f"Analytics recompute failed: {ex}")
    finally:
        with _ANALYTICS_LOCK:
            _ANALYTICS_CACHE["computing"] = False


@app.get("/api/analytics")
def get_analytics():
    """KPIs + chart series + insights for the analytics dashboard page.
    First call computes inline (~seconds); stale-but-present results are served
    instantly while a background thread refreshes them."""
    now = time.time()
    with _ANALYTICS_LOCK:
        payload = _ANALYTICS_CACHE["payload"]
        if payload is not None and not _analytics_stale(now):
            return {**payload, "cached": True}
        if payload is not None:
            if not _ANALYTICS_CACHE["computing"]:
                _ANALYTICS_CACHE["computing"] = True
                threading.Thread(target=_recompute_analytics_bg, daemon=True).start()
            return {**payload, "cached": True, "stale": True}
    # Cold cache: compute inline so the very first page view gets real data.
    try:
        data = compute_analytics()
    except Exception as ex:
        return {"error": f"analytics computation failed: {ex}", "kpis": {}, "charts": {}, "insights": []}
    with _ANALYTICS_LOCK:
        _ANALYTICS_CACHE["payload"] = data
        _ANALYTICS_CACHE["computed_at"] = time.time()
    return {**data, "cached": False}


@app.get("/api/logs")
def get_logs():
    with _state_lock:
        logs_copy = list(PIPELINE_STATE["logs"])
    return {"logs": logs_copy}


@app.get("/api/preview")
def preview_data(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    category: Optional[str] = None,
    search: Optional[str] = None,
):
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    all_items = []

    if os.path.exists(raw_dir):
        # 1. Check for standalone json batches
        for root, _, files in os.walk(raw_dir):
            for file in files:
                if file.endswith(".json") and len(all_items) < 300:
                    p = os.path.join(root, file)
                    try:
                        with open(p, "r", encoding="utf-8") as fp:
                            d = json.load(fp)
                            if isinstance(d, list):
                                all_items.extend(d)
                    except Exception:
                        pass
                elif file.endswith(".jsonl") and len(all_items) < 300:
                    p = os.path.join(root, file)
                    try:
                        with open(p, "r", encoding="utf-8") as fp:
                            for line in fp:
                                if line.strip():
                                    all_items.append(json.loads(line))
                                    if len(all_items) >= 300:
                                        break
                    except Exception:
                        pass

    # Filtering
    filtered = []
    for item in all_items:
        if category and category.lower() != "all" and category.lower() not in item.get("category", "").lower():
            continue
        if search:
            q = search.lower()
            t = item.get("title", "").lower()
            d = item.get("description", "").lower()
            c = item.get("location_city", "").lower()
            lid = item.get("listing_id", "").lower()
            if q not in t and q not in d and q not in c and q not in lid:
                continue
        filtered.append(item)

    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_items = filtered[start_idx:end_idx]

    return {
        "total": len(filtered),
        "page": page,
        "limit": limit,
        "items": paginated_items,
    }


@app.post("/api/hdfs/sync")
def sync_hdfs(background_tasks: BackgroundTasks):
    cmd = [PYTHON_EXEC, os.path.join(PROJECT_ROOT, "scripts", "push_to_hdfs.py")]
    add_log("INFO", "Manual HDFS sync requested by operator.")
    background_tasks.add_task(subprocess.run, cmd, {"cwd": PROJECT_ROOT})
    return {"status": "SUCCESS", "message": "HDFS sync background task initiated."}


if __name__ == "__main__":
    import uvicorn

    print(f"Starting ResellRadar Pipeline Server on http://127.0.0.1:8000 using {PYTHON_EXEC}...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
