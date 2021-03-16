class AvroDataFrameSerializerDeserializer:
    AVRO_FORMAT = "avro"
    OVERWRITE_MODE = "overwrite"

    def __init__(self, spark_configuration):
        self._spark_configuration = spark_configuration

    def serialize(self, data_frame, file_path):
        data_frame.write.save(file_path, format=self.AVRO_FORMAT, mode=self.OVERWRITE_MODE)

    def deserialize(self, file_path):
        spark_session = self._spark_configuration.get_spark_session()
        return spark_session.read.format(self.AVRO_FORMAT).load(file_path)
