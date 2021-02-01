from processors.processor import Processor
from results.integer_results import IntegerResults


class IntegerProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        number_statistics = self._column_statistics_calculator.calculate_number_statistics(key_value_rdd_cached)
        return IntegerResults(number_statistics=number_statistics,
                              count_distinct=key_value_rdd_cached.distinct().count(),
                              entropy=self._column_statistics_calculator.calculate_entropy(key_value_rdd_cached))
