from typing import Dict

from processors.processor import Processor


class FieldDispatcher:
    WITHOUT_REPLACEMENT = "false"
    def __init__(self, type_processors: Dict["SparkType", Processor]):
        self._type_processors = type_processors

    def dispatch(self, column_rdd, column_data_type):
        return self._type_processors[column_data_type].process(column_rdd)
