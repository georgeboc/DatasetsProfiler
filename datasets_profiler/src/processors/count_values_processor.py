from pyspark.sql.functions import lit, sum

from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.results.count_values_results import CountValuesResults


class CountValuesProcessor:
    VALUE = "Value"
    COUNT = "Count"
    FREQUENCY = "Frequency"

    def __init__(self, column_statistics_calculator, spark_configuration, call_tracker):
        self._column_statistics_calculator = column_statistics_calculator
        self._spark_configuration = spark_configuration
        self._call_tracker = call_tracker

    @instrument_call
    def process(self, column_data_frame):
        column_data_frame_cached = column_data_frame.cache()
        map_data_frame = column_data_frame_cached.withColumn(self.COUNT, lit(1))
        renamed_data_frame = map_data_frame.toDF(self.VALUE, self.COUNT)
        values_frequencies_data_frame_cached = renamed_data_frame.groupBy(self.VALUE) \
            .agg(sum(self.COUNT).alias(self.FREQUENCY)) \
            .cache()
        rows_count = values_frequencies_data_frame_cached.count()
        is_unique = rows_count == column_data_frame_cached.count()
        return CountValuesResults(data_frame=values_frequencies_data_frame_cached.orderBy(self.VALUE),
                                  rows_count=rows_count,
                                  is_unique=is_unique)
