from pyspark.sql import SparkSession


class SparkConfiguration:
    def __init__(self, local_execution_checker):
        self._local_execution_checker = local_execution_checker

    def get_spark_context(self):
        return self.get_spark_session().sparkContext

    def get_spark_session(self):
        spark_session_builder = self._get_spark_session_builder()
        spark_session_builder_with_master = self._add_master(spark_session_builder)
        spark_session = spark_session_builder_with_master.getOrCreate()
        spark_session.sparkContext.setLogLevel("ERROR")
        return spark_session

    def _get_spark_session_builder(self):
        return SparkSession.builder \
            .config("spark.memory.offHeap.enabled", True) \
            .config("spark.memory.offHeap.size", "16g") \
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")

    def _add_master(self, spark_session_builder):
        if self._local_execution_checker.is_local_execution():
            return spark_session_builder.master("local[*]")
        return spark_session_builder.master("yarn")
