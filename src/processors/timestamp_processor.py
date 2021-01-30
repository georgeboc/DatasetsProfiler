from processors.processor import Processor


class TimestampProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        pass
