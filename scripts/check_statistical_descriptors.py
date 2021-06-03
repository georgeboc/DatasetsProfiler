from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Check Dataset Description") \
        .config("master", "client") \
        .config("spark.driver.memory", "22602M") \
        .config("spark.driver.memoryOverhead", "1702M") \
        .config("spark.executor.memory", "22602M") \
        .config("spark.executor.memoryOverhead", "1702M") \
        .config("spark.driver.cores", "8") \
        .config("spark.executor.cores", "8") \
        .config("spark.executor.instances", "3") \
        .getOrCreate()

    parquet_file = spark.read.format("parquet").load("../output/samples_correct/described_datasets/Ad_click_on_taobao_sample_10/parsed_dataset.parquet")
    parquet_file.describe().show()
