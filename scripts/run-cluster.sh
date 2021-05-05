#!/bin/bash
# Execute from Dataset Profiler root directory

export DATASETS_PROFILER_WHEEL_PATH="dist/DatasetsProfiler-1.*-py3-none-any.whl"

pip3 install --force-reinstall $DATASETS_PROFILER_WHEEL_PATH
spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.driver.memory 2526 \
--conf spark.yarn.driver.memoryOverhead 512 \
--conf spark.executor.memory 2526 \
--conf spark.yarn.executor.memoryOverhead 512 \
--conf spark.driver.cores 1 \
--conf spark.executor.cores 1 \
--conf spark.executor.instances 23 \
--packages org.apache.spark:spark-avro_2.12:3.0.1 \
--py-files $DATASETS_PROFILER_WHEEL_PATH \
--packages org.apache.spark:spark-avro_2.12:3.0.1 \
--py-files $DATASETS_PROFILER_WHEEL_PATH \
__main__.py --parameters-path "$@"
