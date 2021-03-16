from datasets_profiler.src.instrumentation.call_tracker import instrument_call


class CountValuesProcessor:
    def __init__(self, column_statistics_calculator, call_tracker, processors_operations_flags):
        self._column_statistics_calculator = column_statistics_calculator
        self._call_tracker = call_tracker
        self._processors_operations_flags = processors_operations_flags

    @instrument_call
    def process(self, column_rdd):
        map_rdd = column_rdd.map(lambda row: (row[0], 1))
        values_frequencies_rdd = map_rdd.reduceByKey(lambda a, b: a + b)
        return values_frequencies_rdd.sortByKey()
