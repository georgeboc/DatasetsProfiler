#!/bin/bash

pip3 install --force-reinstall ../dist/DatasetsProfiler-1.*-py3-none-any.whl
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1 \
  --py-files ../dist/DatasetsProfiler-1.*-py3-none-any.whl ../__main__.py "$@"
