from io import BytesIO

from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.schema import make_avsc_object


class AvroDataFrameSerializerDeserializer:
    AVRO = "avro"
    OVERWRITE = "overwrite"
    NO_EAGER_MEMORY_LOAD = False
    DATA_FRAME = "data_frame"
    TYPES_MAPPING = {
        "str": "string"
    }

    def __init__(self, spark_configuration, directories_auxiliary, filesystem):
        self._spark_configuration = spark_configuration
        self._directories_auxiliary = directories_auxiliary
        self._filesystem = filesystem

    def serialize(self, dictionary, file_path):
        self._directories_auxiliary.try_create_directory(file_path)
        data_frame = dictionary[self.DATA_FRAME]
        data_frame.write.save(file_path, format=self.AVRO, mode=self.OVERWRITE, memory=self.NO_EAGER_MEMORY_LOAD)
        self._write_metadata(dictionary, file_path)

    def deserialize(self, file_path):
        spark_session = self._spark_configuration.get_spark_session()
        return spark_session.read.load(file_path, format=self.AVRO, memory=self.NO_EAGER_MEMORY_LOAD)

    def _write_metadata(self, dictionary, directory_path):
        metadata = self._get_metadata(dictionary)
        full_file_path = self._get_metadata_full_file_path(directory_path)
        with BytesIO() as file:
            with DataFileWriter(file, DatumWriter(), self._get_schema(metadata)) as writer:
                writer.append(metadata)
                writer.flush()
                self._filesystem.write(file.getvalue(), full_file_path)

    def _get_metadata(self, dictionary):
        return {key: str(value) for key, value in dictionary.items() if key != self.DATA_FRAME}

    def _get_metadata_full_file_path(self, directory_path):
        return f"{directory_path}/metadata.avro"

    def _get_schema(self, dictionary, name="avro.schema", schema_type="record"):
        return make_avsc_object({
            "name": name,
            "type": schema_type,
            "fields": [{"name": field_name, "type": self.TYPES_MAPPING[type(value).__name__]}
                       for field_name, value in dictionary.items()]
        })
