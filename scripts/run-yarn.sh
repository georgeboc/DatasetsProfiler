#!/bin/bash

pip3 install --force-reinstall ../dist/DatasetsProfiler-1.*-py3-none-any.whl
spark-submit --master yarn --deploy-mode cluster --packages org.apache.spark:spark-avro_2.12:3.0.1 \
  --py-files ../dist/DatasetsProfiler-1.*-py3-none-any.whl /usr/local/lib/python3.8/dist-packages/datasets_profiler/__main__.py "$@"
