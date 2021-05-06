#!/bin/bash
# Execute from Dataset Profiler root directory

export DATASETS_PROFILER_WHEEL_PATH="dist/DatasetsProfiler-1.*-py3-none-any.whl"

pip3 install --force-reinstall $DATASETS_PROFILER_WHEEL_PATH
spark-submit \
--master yarn \
--deploy-mode client \
--conf spark.driver.memory="22602M" \
--conf spark.driver.memoryOverhead="1702M" \
--conf spark.executor.memory="22602M" \
--conf spark.executor.memoryOverhead="1702M" \
--conf spark.driver.cores="8" \
--conf spark.executor.cores="8" \
--conf spark.executor.instances="3" \
--packages org.apache.spark:spark-avro_2.12:3.0.1 \
--py-files $DATASETS_PROFILER_WHEEL_PATH \
__main__.py --parameters-path "$@"
