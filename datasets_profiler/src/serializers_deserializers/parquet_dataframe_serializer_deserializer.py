class ParquetDataframeSerializerDeserializer:
    PARQUET_FORMAT = "parquet"
    OVERWRITE_MODE = "overwrite"
    NO_EAGER_MEMORY_LOAD = False

    def __init__(self, spark_configuration):
        self._spark_configuration = spark_configuration

    def serialize(self, data_frame, file_path):
        data_frame.write.save(file_path, format=self.PARQUET_FORMAT, mode=self.OVERWRITE_MODE,
                              memory=self.NO_EAGER_MEMORY_LOAD)

    def deserialize(self, file_path):
        spark_session = self._spark_configuration.get_spark_session()
        return spark_session.read.load(file_path, format=self.PARQUET_FORMAT, memory=self.NO_EAGER_MEMORY_LOAD)
