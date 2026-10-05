#!/usr/bin/env bash
# ResellRadar - run a Spark pipeline stage inside Docker (Person 1 acceptance harness).
#
# WHY THIS EXISTS
#   The pipeline jobs read/write the real single-node HDFS cluster at
#   hdfs://namenode:9000. On this Windows host that is not reachable:
#     * the host cannot resolve the compose-internal hostnames `namenode`/`datanode`;
#     * the DataNode data-transfer port (9866) is not published, so no host process
#       can pull block data even if DNS were fixed;
#     * Spark on Windows additionally needs `winutils.exe` just to write parquet.
#   Running the *unmodified* job files inside a Spark 4.2.0 container that joins the
#   compose network reproduces the environment the jobs were written for: in-network
#   DNS, in-network block transfer, Linux (no winutils), and the same Spark version
#   as the host's pyspark (4.2.0).
#
#   This harness does NOT modify the pipeline code. It only changes where the job is
#   executed, exactly like running it on a machine with host-to-HDFS connectivity.
#
# USAGE (from the project root, cluster already up via `docker compose up -d`)
#   scripts/run_stage_in_docker.sh spark_jobs/clean_normalize.py
#   scripts/run_stage_in_docker.sh spark_jobs/entity_resolution.py
#   scripts/run_stage_in_docker.sh spark_jobs/feature_engineering.py
#
#   Override knobs:
#     DRIVER_MEMORY=8g SPARK_IMAGE=... HDFS_NETWORK=... scripts/run_stage_in_docker.sh <job>
#     scripts/run_stage_in_docker.sh <job> --conf spark.sql.shuffle.partitions=64
#     DETACH=1 scripts/run_stage_in_docker.sh <job>     # background; follow with
#                                                       #   docker logs -f resellradar-spark-<job>
#     CONTAINER_NAME=my-run DETACH=1 scripts/run_stage_in_docker.sh <job>
#
#   DISTRIBUTED BATCH MODE (multiple processing units instead of one local JVM):
#     CLUSTER=1 scripts/run_stage_in_docker.sh <job>
#     - submits to the Spark Standalone master started by the compose "cluster"
#       profile:  docker compose -f docker-compose.yml --profile cluster up -d \
#                   --scale spark-worker=3
#     - executors run on the worker containers (multi-unit parallelism) and read
#       their inputs from HDFS; job outputs go to HDFS too (RR_DATA_ROOT, default
#       hdfs://namenode:9000/data). The job files stay unmodified: spark-submit
#       --master overrides their .master("local[*]"), and RR_DATA_ROOT redirects
#       the local data/ paths in the jobs to the matching HDFS zones.
#     - runs as root because the HDFS /data zones are owned by root:supergroup.
#     - knobs: SPARK_MASTER_URL EXECUTOR_MEMORY EXECUTOR_CORES RR_DATA_ROOT
#
#   The default image is the local `resellradar/spark:4.2.0-numpy` (docker/spark/Dockerfile,
#   stock apache/spark:4.2.0 + numpy, which pyspark.ml needs); it is built automatically
#   on first use.
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <spark_jobs/xxx.py> [extra spark-submit options]" >&2
  exit 2
fi

JOB="$1"
shift

SPARK_IMAGE="${SPARK_IMAGE:-resellradar/spark:4.2.0-numpy}"
HDFS_NETWORK="${HDFS_NETWORK:-resellradar_default}"

DOCKER="docker"
if ! command -v docker >/dev/null 2>&1; then
  DOCKER="/c/Program Files/Docker/Docker/resources/bin/docker.exe"
fi

CLUSTER="${CLUSTER:-0}"
if [ "$CLUSTER" = "1" ]; then
  SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
  EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-1g}"
  EXECUTOR_CORES="${EXECUTOR_CORES:-3}"
  RR_DATA_ROOT="${RR_DATA_ROOT:-hdfs://namenode:9000/data}"
  # In cluster mode the driver only coordinates (the executors do the heavy work).
  DRIVER_MEMORY="${DRIVER_MEMORY:-1536m}"
  if ! "$DOCKER" ps --format '{{.Names}}' | grep -qx 'resellradar-spark-master'; then
    echo "ERROR: Spark Standalone master is not running. Start the batch cluster first:" >&2
    echo "  docker compose -f docker-compose.yml --profile cluster up -d --scale spark-worker=3" >&2
    exit 1
  fi
else
  DRIVER_MEMORY="${DRIVER_MEMORY:-6g}"
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Git Bash reports paths as /e/Projects/... ; Docker Desktop wants E:/Projects/...
case "$PROJECT_ROOT" in
  /[a-zA-Z]/*) HOST_ROOT="$(printf '%s' "$PROJECT_ROOT" | sed -E 's#^/([a-zA-Z])/#\1:/#')" ;;
  *)           HOST_ROOT="$PROJECT_ROOT" ;;
esac

# Stop Git Bash from rewriting container paths (/work) into Windows paths (C:/...).
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

if ! "$DOCKER" image inspect "$SPARK_IMAGE" >/dev/null 2>&1; then
  echo ">> building $SPARK_IMAGE from docker/spark/Dockerfile"
  "$DOCKER" build -t "$SPARK_IMAGE" "${HOST_ROOT}/docker/spark"
fi

# Long stages (entity_resolution.py) easily outlive a terminal session, so DETACH=1
# starts the container in the background instead of streaming to this shell.
CONTAINER_NAME="${CONTAINER_NAME:-resellradar-spark-$(basename "$JOB" .py)}"
RUN_ARGS=(--rm)
if [ "${DETACH:-0}" = "1" ]; then
  RUN_ARGS=(-d --name "$CONTAINER_NAME")
elif [ "$CLUSTER" = "1" ]; then
  # A cluster-mode driver is addressed by name by the executors (spark.driver.host),
  # so it always gets a name/hostname; the timestamp keeps repeated sync runs unique.
  CONTAINER_NAME="$CONTAINER_NAME-$(date +%s)"
  RUN_ARGS=(--rm --name "$CONTAINER_NAME")
fi
if [ "$CLUSTER" = "1" ]; then
  # root: the HDFS /data zones are owned by root:supergroup. Hostname: valid RFC
  # hostname (Docker forbids '_' in --hostname), resolvable by the worker containers.
  DRIVER_HOSTNAME="$(printf '%s' "$CONTAINER_NAME" | tr -c 'A-Za-z0-9.-' '-')"
  RUN_ARGS+=(--user root --hostname "$DRIVER_HOSTNAME")
  ENV_ARGS=(-e "RR_DATA_ROOT=$RR_DATA_ROOT" -e "SPARK_MASTER=$SPARK_MASTER_URL")
else
  ENV_ARGS=()
fi

echo ">> image   : $SPARK_IMAGE"
echo ">> network : $HDFS_NETWORK"
echo ">> mount   : $HOST_ROOT -> /work"
echo ">> job     : $JOB"
if [ "$CLUSTER" = "1" ]; then
  echo ">> cluster : $SPARK_MASTER_URL  (executor: $EXECUTOR_CORES cores / $EXECUTOR_MEMORY;  data root: $RR_DATA_ROOT)"
fi

SUBMIT_ARGS=(--driver-memory "$DRIVER_MEMORY" --conf spark.ui.enabled=false)
if [ "$CLUSTER" = "1" ]; then
  # --master and SPARK_MASTER both point the driver at the standalone master
  # (code-set .master() in a job takes precedence over the flag, so the env var
  # is what actually routes the SparkSession; the flag covers plain spark-submit runs).
  SUBMIT_ARGS+=(
    --master "$SPARK_MASTER_URL"
    --executor-memory "$EXECUTOR_MEMORY"
    --executor-cores "$EXECUTOR_CORES"
    --conf spark.driver.bindAddress=0.0.0.0
    # This client container has no hdfs-site.xml, so HDFS writes would otherwise use
    # the built-in default (replication 3) on our replication-1 cluster.
    --conf spark.hadoop.dfs.replication=1
  )
fi

exec "$DOCKER" run "${RUN_ARGS[@]}" \
  --network "$HDFS_NETWORK" \
  "${ENV_ARGS[@]}" \
  -v "${HOST_ROOT}:/work" \
  -w /work \
  "$SPARK_IMAGE" \
  /opt/spark/bin/spark-submit \
    "${SUBMIT_ARGS[@]}" \
    "$@" "$JOB"
