from configuration.execute_if_flag_is_enabled import execute_if_flag_is_enabled
from instrumentation.call_tracker import instrument_call
from processors.processor import Processor
from pyspark.sql.functions import monotonically_increasing_id, lag, when, isnull
from pyspark.sql.window import Window

from results.timestamp_results import TimestampResults


class TimestampProcessor(Processor):
    VALUE_FIELD = "_2"
    ID_FIELD = "id"
    PREVIOUS_VALUE_FIELD = "previous_value"
    DIFF_IN_SECONDS_FIELD = "diff_in_seconds"
    DEFAULT_FIRST_COL_NAME = "_1"
    DEFAULT_SECOND_COL_NAME = "_2"
    DATETIME_CAST = "long"

    def __init__(self, column_statistics_calculator, call_tracker, processors_operations_flags):
        self._column_statistics_calculator = column_statistics_calculator
        self._call_tracker = call_tracker
        self._processors_operations_flags = processors_operations_flags

    @instrument_call
    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        if key_value_rdd_cached.isEmpty():
            return TimestampResults()
        not_null_key_value_rdd_cached = key_value_rdd_cached.filter(lambda row: row[1] is not None).cache()
        delta_time_in_seconds_rdd_cached = self._get_delta_time_in_seconds(not_null_key_value_rdd_cached).cache()
        oldest_newest_date = self._column_statistics_calculator.calculate_min_max(not_null_key_value_rdd_cached)
        oldest_date, newest_date = oldest_newest_date if oldest_newest_date is not None else (None, None)
        return TimestampResults(delta_time_in_seconds_statistics=self._get_delta_time_in_seconds_statistics(delta_time_in_seconds_rdd_cached),
                                count_distinct=self._calculate_distinct_rows_count(not_null_key_value_rdd_cached),
                                newest_date=newest_date,
                                oldest_date=oldest_date,
                                timestamp_entropy=self._calculate_timestamp_entropy(not_null_key_value_rdd_cached),
                                delta_time_in_seconds_entropy=self._calculate_delta_time_in_seconds_entropy(delta_time_in_seconds_rdd_cached),
                                count_null=self._calculate_null_rows_count(key_value_rdd_cached),
                                count_not_null=self._calculate_not_null_rows_count(not_null_key_value_rdd_cached))

    @execute_if_flag_is_enabled("timestamp_processor_get_delta_time_in_seconds_statistics_is_enabled")
    @instrument_call
    def _get_delta_time_in_seconds_statistics(self, delta_time_in_seconds_rdd_cached):
        return self._column_statistics_calculator.calculate_number_statistics(delta_time_in_seconds_rdd_cached)

    @execute_if_flag_is_enabled("timestamp_processor_calculate_not_null_rows_count_is_enabled")
    @instrument_call
    def _calculate_not_null_rows_count(self, not_null_key_value_rdd_cached):
        return not_null_key_value_rdd_cached.count()

    @execute_if_flag_is_enabled("timestamp_processor_calculate_delta_time_in_seconds_entropy_is_enabled")
    @instrument_call
    def _calculate_delta_time_in_seconds_entropy(self, delta_time_in_seconds_rdd_cached):
        return self._column_statistics_calculator.calculate_entropy(delta_time_in_seconds_rdd_cached)

    @execute_if_flag_is_enabled("timestamp_processor_calculate_timestamp_entropy_is_enabled")
    @instrument_call
    def _calculate_timestamp_entropy(self, not_null_key_value_rdd_cached):
        return self._column_statistics_calculator.calculate_entropy(not_null_key_value_rdd_cached)

    @execute_if_flag_is_enabled("timestamp_processor_calculate_distinct_rows_count_is_enabled")
    @instrument_call
    def _calculate_distinct_rows_count(self, not_null_key_value_rdd_cached):
        return not_null_key_value_rdd_cached.distinct().count()

    @execute_if_flag_is_enabled("timestamp_processor_calculate_null_rows_count_is_enabled")
    @instrument_call
    def _calculate_null_rows_count(self, key_value_rdd_cached):
        return key_value_rdd_cached.filter(lambda row: row[1] is None).count()

    def _get_delta_time_in_seconds(self, rdd_cached):
        data_frame = rdd_cached.toDF()
        window = Window.partitionBy().orderBy(self.VALUE_FIELD)
        added_previous_value = data_frame.withColumn(self.PREVIOUS_VALUE_FIELD, lag(data_frame._2).over(window)).rdd
        return added_previous_value.map(self._timestamp_diff).filter(bool)

    def _timestamp_diff(self, row):
        key, value, previous_value = row
        if previous_value is None:
            return
        return key, (value - previous_value).total_seconds()
