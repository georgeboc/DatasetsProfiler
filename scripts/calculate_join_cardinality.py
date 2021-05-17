# Execute from Dataset Profiler root directory
import re
import json
from decimal import Decimal
from quantiphy import Quantity

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from datasets_profiler.src.dependency_injection.containers import FilesystemProviders

VALUE = "Value"
FREQUENCY = "Frequency"
LEFT_FREQUENCY = "LeftFrequency"
RIGHT_FREQUENCY = "RightFrequency"
MULTIPLIED = "Multiplied"

# Input example: /user/bochileanu/described_datasets/Ad_click_on_taobao_1g
left_dataset_path = input("Insert left described dataset path (use without prefix hdfs://host:port): ")
right_dataset_path = input("Insert right described dataset path (use without prefix hdfs://host:port): ")
join_left_column_name = input("Insert join left column name: ")
join_right_column_name = input("Insert join right column name: ")
left_dataset_size = float(input("Introduce left dataset size in MB: "))

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
    .config("spark.jars", "scripts/spark-avro_2.12-3.1.1.jar") \
    .getOrCreate()

filesystem = FilesystemProviders.hdfs_filesystem()
left_count_value_stat = f"count_value_stats_{join_left_column_name}"
right_count_value_stat = f"count_value_stats_{join_right_column_name}"
left_parquet_file = spark.read.format("avro")\
    .load(f"{left_dataset_path}/{left_count_value_stat}/part*")\
    .withColumnRenamed(FREQUENCY, LEFT_FREQUENCY)
right_parquet_file = spark.read.format("avro")\
    .load(f"{right_dataset_path}/{right_count_value_stat}/part*")\
    .withColumnRenamed(FREQUENCY, RIGHT_FREQUENCY)

print("Left parquet file:")
left_parquet_file.show()

print("Right parquet file:")
right_parquet_file.show()

joined_data_set = left_parquet_file.join(right_parquet_file, VALUE).cache()
joined_data_set.show()

print("Selected columns:")
selected_columns = joined_data_set.select(col(LEFT_FREQUENCY), col(RIGHT_FREQUENCY)).cache()
selected_columns.show()

print("Multiplication result:")
multiplied = selected_columns.withColumn(MULTIPLIED, col(LEFT_FREQUENCY)*col(RIGHT_FREQUENCY)).cache()
multiplied.show()

result = sum(multiplied.select(MULTIPLIED).rdd.reduce(lambda a, b: a + b))

print("Summary")
print(result)

print()
print("Increased size percentage:")
dataset_rows_count = spark.read.format("avro").load(f"{left_dataset_path}/{left_count_value_stat}/part*").count()
print(f"{str((result / dataset_rows_count - 1) * 100)}%")

print()
print("Estimated output size:")
print(str(Quantity(result * left_dataset_size / dataset_rows_count * 2**20, "B")))

print()
print("Left dataset rows count:")
print(dataset_rows_count)
