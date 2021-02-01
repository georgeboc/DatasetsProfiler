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
        count_distinct = key_value_rdd_cached.distinct().count()
        oldest_date, newest_date = self._column_statistics_calculator.calculate_min_max(key_value_rdd_cached)
        delta_time_in_seconds_rdd_cached = self._get_delta_time_in_seconds(key_value_rdd_cached).cache()
        delta_time_in_seconds_statistics=self._column_statistics_calculator.calculate_number_statistics(delta_time_in_seconds_rdd_cached)
        return TimestampResults(delta_time_in_seconds_statistics=delta_time_in_seconds_statistics,
                                count_distinct=count_distinct,
                                newest_date=newest_date,
                                oldest_date=oldest_date,
                                count=key_value_rdd_cached.count(),
                                timestamp_entropy=self._column_statistics_calculator.calculate_entropy(key_value_rdd_cached),
                                delta_time_in_seconds_entropy=self._column_statistics_calculator.calculate_entropy(delta_time_in_seconds_rdd_cached))

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
                                                                           added_previous_value_data_frame.value.cast(self.DATETIME_CAST) -
                                                                           added_previous_value_data_frame.previous_value.cast(self.DATETIME_CAST)
                                                                       ),
                                                                       0)
                                                                   .otherwise(added_previous_value_data_frame.value.cast(self.DATETIME_CAST) -
                                                                              added_previous_value_data_frame.previous_value.cast(self.DATETIME_CAST))
                                                                )
        delta_time_only = delta_time.select(self.DEFAULT_FIRST_COL_NAME, self.DIFF_IN_SECONDS_FIELD)
        return delta_time_only.rdd
