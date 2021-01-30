from typing import Dict

from processors.processor import Processor


class FieldDispatcher:
    def __init__(self, type_processors: Dict["SparkType", Processor]):
        self._type_processors = type_processors

    def dispatch(self, column_rdd):
        column_data_frame = column_rdd.toDF()
        field = column_data_frame.schema.fields[0]
        return self._type_processors[field.dataType].process(column_rdd)
