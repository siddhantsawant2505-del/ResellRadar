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
DRIVER_MEMORY="${DRIVER_MEMORY:-6g}"

DOCKER="docker"
if ! command -v docker >/dev/null 2>&1; then
  DOCKER="/c/Program Files/Docker/Docker/resources/bin/docker.exe"
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
fi

echo ">> image   : $SPARK_IMAGE"
echo ">> network : $HDFS_NETWORK"
echo ">> mount   : $HOST_ROOT -> /work"
echo ">> job     : $JOB"

exec "$DOCKER" run "${RUN_ARGS[@]}" \
  --network "$HDFS_NETWORK" \
  -v "${HOST_ROOT}:/work" \
  -w /work \
  "$SPARK_IMAGE" \
  /opt/spark/bin/spark-submit \
    --driver-memory "$DRIVER_MEMORY" \
    --conf spark.ui.enabled=false \
    "$@" "$JOB"
