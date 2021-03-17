from pyspark.sql.functions import lit, sum

from datasets_profiler.src.instrumentation.call_tracker import instrument_call


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
        map_data_frame = column_data_frame.withColumn(self.COUNT, lit(1))
        renamed_data_frame = map_data_frame.toDF(self.VALUE, self.COUNT)
        values_frequencies_data_frame = renamed_data_frame.groupBy(self.VALUE).agg(sum(self.COUNT).alias(self.FREQUENCY))
        return values_frequencies_data_frame.orderBy(self.VALUE)
