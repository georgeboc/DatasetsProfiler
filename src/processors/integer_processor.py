from processors.processor import Processor
from results.integer_results import IntegerResults


class IntegerProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value)).cache
        number_statistics = self._column_statistics_calculator.calculate(key_value_rdd_cached)
        count_distinct = self._column_statistics_calculator.count_distinct(key_value_rdd_cached)
        return IntegerResults(number_statistics=number_statistics,
                              count_distinct=count_distinct)
