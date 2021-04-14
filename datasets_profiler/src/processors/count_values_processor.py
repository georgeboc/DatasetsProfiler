from pyspark.sql.functions import lit, sum, col, unix_timestamp
from pyspark.sql.types import StringType

from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.results.count_values_results import CountValuesResults


class CountValuesProcessor:
    VALUE = "Value"
    COUNT = "Count"
    FREQUENCY = "Frequency"
    TIMESTAMP = "timestamp"
    LONG = "long"
    TIMESTAMP_TYPE = "timestamp"

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
        unix_timestamps_data_frame = self.cast_timestamp_to_timestamp_in_milliseconds(values_frequencies_data_frame_cached)
        string_casted_data_frame = unix_timestamps_data_frame.withColumn(self.VALUE, col(self.VALUE).cast(StringType()))
        sorted_data_frame = string_casted_data_frame.orderBy(self.VALUE)
        return CountValuesResults(data_frame=sorted_data_frame,
                                  rows_count=rows_count,
                                  is_unique=is_unique)

    def cast_timestamp_to_timestamp_in_milliseconds(self, data_frame):
        columns_types = data_frame.dtypes
        column_type = columns_types[0]
        type = column_type[1]
        if type == self.TIMESTAMP_TYPE:
            return data_frame.withColumn(self.VALUE, unix_timestamp(col(self.VALUE))*1000)
        return data_frame
