# ResellRadar - Spark acceptance image (Person 1)
#
# WHY: the pipeline stages are run by scripts/run_stage_in_docker.sh inside a Spark
# container that joins the HDFS compose network (see that script for the reasoning).
# The stock apache/spark image ships WITHOUT numpy, and `pyspark.ml` imports numpy at
# module load - so entity_resolution.py fails on `from pyspark.ml.feature import
# HashingTF, MinHashLSH` with ModuleNotFoundError. This thin layer adds numpy so the
# uploaded job files run completely unmodified.
#
# Base tag is pinned to Spark 4.2.0 (scala 2.13 / java 17 / python 3) to match the
# pyspark version the jobs were written against.
#
# Build:  docker build -t resellradar/spark:4.2.0-numpy docker/spark
# (scripts/run_stage_in_docker.sh builds it automatically if it is missing.)

FROM apache/spark:4.2.0-scala2.13-java17-python3-ubuntu

USER root

RUN pip3 install --no-cache-dir numpy \
    || PIP_BREAK_SYSTEM_PACKAGES=1 pip3 install --no-cache-dir numpy

USER spark
