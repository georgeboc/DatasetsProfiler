from pyspark.sql import SparkSession

parquet_path = input("Insert path to parquet file (use prefix file:/// or hdfs://host:port/): ")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master", "client") \
    .config("spark.driver.memory", "2526M") \
    .config("spark.yarn.driver.memoryOverhead", "512M") \
    .config("spark.executor.memory", "2526M") \
    .config("spark.yarn.executor.memoryOverhead", "512M") \
    .config("spark.driver.cores", 1) \
    .config("spark.executor.cores", 1) \
    .config("spark.executor.instances", 23) \
    .getOrCreate()

parquetFile = spark.read.parquet(parquet_path)
print(parquetFile.count())
