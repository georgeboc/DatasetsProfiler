from processors.processor import Processor
from pyspark.sql import functions
from pyspark.sql.window import Window

class TimestampProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        key_value_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        distinct_count = key_value_rdd_cached.distinct().count()



        key_value_data_frame = key_value_rdd_cached.toDF()
        key_value_data_frame_renamed = key_value_data_frame.withColumnRenamed("_2", "value")
        key_value_with_id = key_value_data_frame_renamed.withColumn("id", functions.monotonically_increasing_id())
        window = Window.partitionBy().orderBy("id")
        key_value_with_prev_value = key_value_with_id.withColumn("prev_value", functions.lag(key_value_with_id.value).over(window))
        key_value_with_diff = key_value_with_prev_value.withColumn("diff_in_seconds",
                                                                   functions.when(
                                                                       functions.isnull(key_value_with_prev_value.value.cast("long") - key_value_with_prev_value.prev_value.cast("long")), 0)
                                                                   .otherwise(key_value_with_prev_value.value.cast("long") - key_value_with_prev_value.prev_value.cast("long")))
        key_value_diff_in_seconds = key_value_with_diff.select("_1", "diff_in_seconds")
        key_value_diff_in_seconds.show()
        self._column_statistics_calculator.calculate(key_value_diff_in_seconds.rdd)
