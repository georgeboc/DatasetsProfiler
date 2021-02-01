from processors.processor import Processor
from results.tuple_results import TupleResults


class TupleProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, rdd):
        key_value_rdd = rdd.map(lambda row: (1, row))
        return TupleResults(entropy=self._column_statistics_calculator.calculate_entropy(key_value_rdd))
