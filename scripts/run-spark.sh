#!/bin/bash

ABSOLUTE_PATH_FILENAME=$(realpath "$0")
SCRIPTS_PATH=$(dirname "$ABSOLUTE_PATH_FILENAME")
ROOT_DIR_LOCAL=$(dirname "$SCRIPTS_PATH")
ROOT_DIR_HDFS="user/root"

hdfs dfs -copyFromLocal -f -t 8 "$ROOT_DIR_LOCAL/resources" "/$ROOT_DIR_HDFS"
hdfs dfs -copyFromLocal -f -t 8 "$ROOT_DIR_LOCAL/dist/DatasetsEvaluation-1.0-py3.9.egg" "/$ROOT_DIR_HDFS"
spark-submit --master yarn --py-files "hdfs://localhost:9000/$ROOT_DIR_HDFS/DatasetsEvaluation-1.0-py3.9.egg" "$ROOT_DIR_LOCAL/src/__main__.py"
