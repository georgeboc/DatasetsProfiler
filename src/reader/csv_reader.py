import pyspark
from pyspark.sql import SparkSession


class CSVReader:
    def read(self, filename, separator=',', header=False):
        spark_context = pyspark.SparkContext.getOrCreate()
        spark_context.setLogLevel("WARN")
        spark_session = SparkSession(spark_context)
        return spark_session.read.load(filename,
                                       format="csv",
                                       sep=separator,
                                       inferSchema="true",
                                       header=str(header))
