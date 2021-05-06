from pyspark.sql import SparkSession

parquet_path = input("Insert path to parquet file (use prefix file:/// or hdfs://host:port/): ")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("master", "client") \
    .config("spark.driver.memory", "22602M") \
    .config("spark.driver.memoryOverhead", "1702M") \
    .config("spark.executor.memory", "22602M") \
    .config("spark.executor.memoryOverhead", "1702M") \
    .config("spark.driver.cores", "8") \
    .config("spark.executor.cores", "8") \
    .config("spark.executor.instances", "3") \
    .getOrCreate()

parquetFile = spark.read.parquet(parquet_path)
print(parquetFile.count())
