from pyspark.sql.functions import col


class AvroDataFrameSerializerDeserializer:
    AVRO_FORMAT = "avro"
    OVERWRITE_MODE = "overwrite"
    NO_EAGER_MEMORY_LOAD = False
    DATA_FRAME = "data_frame"
    METADATA_COLUMN = "Frequency"

    def __init__(self, spark_configuration, directories_auxiliary):
        self._spark_configuration = spark_configuration
        self._directories_auxiliary = directories_auxiliary

    def serialize(self, dictionary, file_path):
        self._directories_auxiliary.try_create_directory(file_path)
        data_frame = dictionary[self.DATA_FRAME]
        data_frame.withColumn(self.METADATA_COLUMN, col(self.METADATA_COLUMN)
                              .alias("", metadata=self._get_metadata(dictionary)))
        data_frame.write.save(f"{file_path}.avro", format=self.AVRO_FORMAT, mode=self.OVERWRITE_MODE,
                              memory=self.NO_EAGER_MEMORY_LOAD)

    def deserialize(self, file_path):
        spark_session = self._spark_configuration.get_spark_session()
        return spark_session.read.load(f"{file_path}.avro", format=self.AVRO_FORMAT, memory=self.NO_EAGER_MEMORY_LOAD)

    def _get_metadata(self, dictionary):
        return {key: value for key, value in dictionary.items() if key != self.DATA_FRAME}
