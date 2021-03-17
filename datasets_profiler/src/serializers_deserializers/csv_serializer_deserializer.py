import csv

from datasets_profiler.src.serializers_deserializers.directories_auxiliary import try_create_directory


class CSVSerializerDeserializer:
    DELIMITER = ";"

    def serialize(self, object, file_path):
        try_create_directory(file_path)
        with open(f"{file_path}.csv", "a") as file_descriptor:
            writer = csv.writer(file_descriptor, delimiter=self.DELIMITER)
            writer.writerows(object)

    def deserialize(self, file_path):
        with open(f"{file_path}.csv", "r") as file_descriptor:
            reader = csv.reader(file_descriptor, delimiter=self.DELIMITER)
            return list(reader)
