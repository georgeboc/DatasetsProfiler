#!/bin/bash

pip3 install --force-reinstall DatasetsProfiler-1.*-py3-none-any.whl
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1 \
  --py-files DatasetsProfiler-1.*-py3-none-any.whl /usr/local/lib/python3.8/dist-packages/datasets_profiler/__main__.py "$@"