from datasets_profiler.src.serializers_deserializers.directories_auxiliary import try_create_directory


class AvroDataFrameSerializerDeserializer:
    AVRO_FORMAT = "avro"
    OVERWRITE_MODE = "overwrite"
    NO_EAGER_MEMORY_LOAD = False

    def __init__(self, spark_configuration):
        self._spark_configuration = spark_configuration

    def serialize(self, data_frame, file_path):
        try_create_directory(file_path)
        data_frame.write.save(f"{file_path}.avro", format=self.AVRO_FORMAT, mode=self.OVERWRITE_MODE,
                              memory=self.NO_EAGER_MEMORY_LOAD)

    def deserialize(self, file_path):
        spark_session = self._spark_configuration.get_spark_session()
        return spark_session.read.load(f"{file_path}.avro", format=self.AVRO_FORMAT, memory=self.NO_EAGER_MEMORY_LOAD)
