from datasets_profiler.src.configuration.execute_if_flag_is_enabled import execute_if_flag_is_enabled
from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.processors.processor import Processor
from datasets_profiler.src.results.tuple_results import TupleResults


class TupleProcessor(Processor):
    def __init__(self, column_statistics_calculator, call_tracker, processors_operations_flags):
        self._column_statistics_calculator = column_statistics_calculator
        self._call_tracker = call_tracker
        self._processors_operations_flags = processors_operations_flags

    @instrument_call
    def process(self, rdd):
        key_value_rdd = rdd.map(lambda row: (1, row))
        return TupleResults(entropy=self._calculate_tuple_entropy(key_value_rdd))

    @execute_if_flag_is_enabled("tuple_processor_calculate_tuple_entropy_is_enabled")
    @instrument_call
    def _calculate_tuple_entropy(self, key_value_rdd):
        return self._column_statistics_calculator.calculate_entropy(key_value_rdd.cache())
