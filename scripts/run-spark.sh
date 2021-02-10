#!/bin/bash

ABSOLUTE_PATH_FILENAME=$(realpath "$0")
SCRIPTS_PATH=$(dirname "$ABSOLUTE_PATH_FILENAME")
ROOT_DIR_LOCAL=$(dirname "$SCRIPTS_PATH")
ROOT_DIR_HDFS="user/root"

#spark-shell --master yarn --executor-memory 7G

hdfs dfs -mkdir -p "/$ROOT_DIR_HDFS"
hdfs dfs -copyFromLocal -f -t 8 "$ROOT_DIR_LOCAL/resources/Datasets/Test/test.log" "/$ROOT_DIR_HDFS"
hdfs dfs -copyFromLocal -f -t 8 "$ROOT_DIR_LOCAL/dist/DatasetsEvaluation-1.0-py3.9.egg" "/$ROOT_DIR_HDFS"

# FUNCTIONAL: spark-submit --py-files "$ROOT_DIR_LOCAL/dist/DatasetsEvaluation-1.0-py3.9.egg" "$ROOT_DIR_LOCAL/src/__main__.py"

spark-submit --py-files "hdfs://localhost:9000/$ROOT_DIR_HDFS/DatasetsEvaluation-1.0-py3.9.egg" "$ROOT_DIR_LOCAL/src/__main__.py"

# --master yarn --num-executors 1 --driver-memory 2g --executor-memory 1g --executor-cores 4
