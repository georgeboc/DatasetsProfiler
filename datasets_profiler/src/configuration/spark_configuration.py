from pyspark.sql import SparkSession


class SparkConfiguration:
    def get_spark_context(self):
        return self.get_spark_session().sparkContext

    def get_spark_session(self):
        spark_session = SparkSession.builder\
            .master("local[*]") \
            .config("spark.driver.maxResultSize", "10g") \
            .config("spark.executor.memory", "16g") \
            .config("spark.driver.memory", "16g") \
            .config("spark.memory.offHeap.enabled", True) \
            .config("spark.memory.offHeap.size", "16g") \
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") \
            .getOrCreate()
        spark_session.sparkContext.setLogLevel("ERROR")
        return spark_session
