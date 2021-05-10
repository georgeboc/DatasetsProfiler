import csv
from io import StringIO


class CSVSerializerDeserializer:
    DELIMITER = ";"
    UTF8 = "utf-8"

    def __init__(self, directories_auxiliary, filesystem):
        self._directories_auxiliary = directories_auxiliary
        self._filesystem = filesystem

    def serialize(self, object, file_path):
        self._directories_auxiliary.try_create_directory(file_path)
        with StringIO() as file:
            writer = csv.writer(file, delimiter=self.DELIMITER)
            writer.writerows(object)
            self._filesystem.write(file.getvalue().encode(self.UTF8), file_path)

    def deserialize(self, file_path):
        value = self._filesystem.read(file_path).decode(self.UTF8)
        with StringIO(value) as file:
            reader = csv.reader(file, delimiter=self.DELIMITER)
            return list(reader)
