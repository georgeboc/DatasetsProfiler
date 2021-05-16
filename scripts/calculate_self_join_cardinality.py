import re
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, pow

from datasets_profiler.src.dependency_injection.containers import FilesystemProviders

FIRST_ELEMENT = 0

# Input example: /user/bochileanu/described_datasets/Ad_click_on_taobao_1g
dataset_path = input("Insert described dataset path (use without prefix hdfs://host:port): ")

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
    .config("spark.jars", "spark-avro_2.12-3.1.1.jar") \
    .getOrCreate()

filesystem = FilesystemProviders.hdfs_filesystem()
sub_folder_names = filesystem.list(dataset_path)

print("Sub folder names:")
print(sub_folder_names)

count_value_stats_pattern = r'count_value_stats_(.*)'
count_value_stats = [sub_folder_name
                     for sub_folder_name in sub_folder_names
                     if bool(re.match(count_value_stats_pattern, sub_folder_name))]

print("Count value stats:")
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
