from processors.processor import Processor
from pyspark.sql.functions import monotonically_increasing_id, lag, when, isnull
from pyspark.sql.window import Window

from results.timestamp_results import TimestampResults


class TimestampProcessor(Processor):
    VALUE_FIELD = "value"
    ID_FIELD = "id"
    PREVIOUS_VALUE_FIELD = "previous_value"
    DIFF_IN_SECONDS_FIELD = "diff_in_seconds"
    DEFAULT_FIRST_COL_NAME = "_1"
    DEFAULT_SECOND_COL_NAME = "_2"
    DATETIME_CAST = "long"

    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        if key_value_rdd_cached.isEmpty():
            return TimestampResults()
        count_null = key_value_rdd_cached.filter(lambda row: row[1] is None).count()
        not_null_key_value_rdd_cached = key_value_rdd_cached.filter(lambda row: row[1] is not None).cache()

        count_distinct = not_null_key_value_rdd_cached.distinct().count()
        oldest_date, newest_date = self._column_statistics_calculator.calculate_min_max(not_null_key_value_rdd_cached)
        delta_time_in_seconds_rdd_cached = self._get_delta_time_in_seconds(not_null_key_value_rdd_cached).cache()
        delta_time_in_seconds_statistics=self._column_statistics_calculator.calculate_number_statistics(delta_time_in_seconds_rdd_cached)
        return TimestampResults(delta_time_in_seconds_statistics=delta_time_in_seconds_statistics,
                                count_distinct=count_distinct,
                                newest_date=newest_date,
                                oldest_date=oldest_date,
                                timestamp_entropy=self._column_statistics_calculator.calculate_entropy(not_null_key_value_rdd_cached),
                                delta_time_in_seconds_entropy=self._column_statistics_calculator.calculate_entropy(delta_time_in_seconds_rdd_cached),
                                count_null=count_null,
                                count_not_null=not_null_key_value_rdd_cached.count())

    def _get_delta_time_in_seconds(self, rdd_cached):
        data_frame = rdd_cached.toDF()
        renamed_data_frame = data_frame.withColumnRenamed(self.DEFAULT_SECOND_COL_NAME, self.VALUE_FIELD)
        added_id_data_frame = renamed_data_frame.withColumn(self.ID_FIELD, monotonically_increasing_id())
        window = Window.partitionBy().orderBy(self.ID_FIELD)
        added_previous_value_data_frame = added_id_data_frame.withColumn(self.PREVIOUS_VALUE_FIELD,
                                                                         lag(added_id_data_frame.value).over(window))
        delta_time = added_previous_value_data_frame.withColumn(self.DIFF_IN_SECONDS_FIELD,
                                                                   when(
                                                                       isnull(
                                                                           self._timestamp_diff(added_previous_value_data_frame.value,
                                                                                                added_previous_value_data_frame.previous_value)
                                                                       ),
                                                                       0)
                                                                   .otherwise(self._timestamp_diff(added_previous_value_data_frame.value,
                                                                              added_previous_value_data_frame.previous_value))
                                                                )
        delta_time_only = delta_time.select(self.DEFAULT_FIRST_COL_NAME, self.DIFF_IN_SECONDS_FIELD)
        return delta_time_only.rdd

    def _timestamp_diff(self, time1, time2):
        return time1.cast(self.DATETIME_CAST) - time2.cast(self.DATETIME_CAST)
