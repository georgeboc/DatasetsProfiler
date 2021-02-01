from processors.processor import Processor
from results.numeric_results import NumericResults


class NumericProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        if key_value_rdd_cached.isEmpty():
            return NumericResults()
        count_null = key_value_rdd_cached.filter(lambda row: row[1] is None).count()
        not_null_key_value_rdd_cached = key_value_rdd_cached.filter(lambda row: row[1] is not None).cache()

        number_statistics = self._column_statistics_calculator.calculate_number_statistics(not_null_key_value_rdd_cached)
        return NumericResults(number_statistics=number_statistics,
                              count_distinct=not_null_key_value_rdd_cached.distinct().count(),
                              entropy=self._column_statistics_calculator.calculate_entropy(not_null_key_value_rdd_cached),
                              count_null=count_null,
                              count_not_null=not_null_key_value_rdd_cached.count())
