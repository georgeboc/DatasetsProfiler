# Execute from Dataset Profiler root directory
import re
import json
from decimal import Decimal
from quantiphy import Quantity

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, pow

from datasets_profiler.src.dependency_injection.containers import FilesystemProviders

FIRST_ELEMENT = 0

# Input example: /user/bochileanu/described_datasets/Ad_click_on_taobao_1g
dataset_path = input("Insert described dataset path (use without prefix hdfs://host:port): ")
dataset_size = float(input("Introduce dataset size in MB: "))

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
sub_folder_names = filesystem.list(dataset_path)

count_value_stats_pattern = r'count_value_stats_(.*)'
count_value_stats = [sub_folder_name
                     for sub_folder_name in sub_folder_names
                     if bool(re.match(count_value_stats_pattern, sub_folder_name))]

print("Count value stats to be processed:")
print(count_value_stats)

results = {}
for count_value_stat in count_value_stats:
    column_name = re.search(count_value_stats_pattern, count_value_stat).groups()[FIRST_ELEMENT]
    print(f"Processing column {column_name}")
    parquetFile = spark.read.format("avro").load(f"{dataset_path}/{count_value_stat}/part*")
    parquetFile.show()
    results[column_name] = int(parquetFile.agg(sum(pow(col("Frequency"), 2))).first()[FIRST_ELEMENT])

print("Summary:")
print(json.dumps(results, indent=4))

print("Increased size percentage:")
dataset_rows_count = spark.read.format("avro").load(f"{dataset_path}/{count_value_stats[FIRST_ELEMENT]}/part*").count()
increased_size_percentage = {}
for column_name, output_rows_count in results.items():
    increased_size_percentage[column_name] = f"{str((output_rows_count / dataset_rows_count - 1) * 100)}%"
print(json.dumps(increased_size_percentage, indent=4))

print("Estimated output size:")
estimated_output_size = {}
for column_name, output_rows_count in results.items():
    estimated_output_size[column_name] = str(Quantity(output_rows_count * dataset_size / dataset_rows_count, "B"))
print(json.dumps(estimated_output_size, indent=4))

print("Dataset rows count:")
print(dataset_rows_count)
