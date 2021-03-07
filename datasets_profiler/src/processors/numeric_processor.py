from datasets_profiler.src.configuration.execute_if_flag_is_enabled import execute_if_flag_is_enabled
from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.processors.processor import Processor
from datasets_profiler.src.results.numeric_results import NumericResults


class NumericProcessor(Processor):
    def __init__(self, column_statistics_calculator, call_tracker, processors_operations_flags):
        self._column_statistics_calculator = column_statistics_calculator
        self._call_tracker = call_tracker
        self._processors_operations_flags = processors_operations_flags

    @instrument_call
    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        if key_value_rdd_cached.isEmpty():
            return NumericResults()
        not_null_key_value_rdd_cached = key_value_rdd_cached.filter(lambda row: row[1] is not None).cache()
        return NumericResults(number_statistics=self._column_statistics_calculator.calculate_number_statistics(not_null_key_value_rdd_cached),
                              count_distinct=self._calculate_distinct_rows_count(not_null_key_value_rdd_cached),
                              entropy=self._column_statistics_calculator.calculate_entropy(not_null_key_value_rdd_cached),
                              count_null=self._calculate_null_rows_count(key_value_rdd_cached),
                              count_not_null=self._calculate_not_null_rows_count(not_null_key_value_rdd_cached))

    @execute_if_flag_is_enabled("numeric_processor_calculate_not_null_rows_count_is_enabled")
    @instrument_call
    def _calculate_not_null_rows_count(self, not_null_key_value_rdd_cached):
        return not_null_key_value_rdd_cached.count()

    @execute_if_flag_is_enabled("numeric_processor_calculate_distinct_is_enabled")
    @instrument_call
    def _calculate_distinct_rows_count(self, not_null_key_value_rdd_cached):
        return not_null_key_value_rdd_cached.distinct().count()

    @execute_if_flag_is_enabled("numeric_processor_calculate_null_rows_count_is_enabled")
    @instrument_call
    def _calculate_null_rows_count(self, key_value_rdd_cached):
        return key_value_rdd_cached.filter(lambda row: row[1] is None).count()
