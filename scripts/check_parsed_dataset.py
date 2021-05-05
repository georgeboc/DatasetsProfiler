from pyspark.sql import SparkSession

parquet_path = input("Insert path to parquet file (use prefix file:/// or hdfs://host:port/): ")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master", "local[*]") \
    .getOrCreate()

parquetFile = spark.read.parquet(parquet_path)
print(parquetFile.count())