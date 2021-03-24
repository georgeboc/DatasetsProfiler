import csv


class CSVSerializerDeserializer:
    DELIMITER = ";"

    def __init__(self, directories_auxiliary):
        self._directories_auxiliary = directories_auxiliary

    def serialize(self, object, file_path):
        self._directories_auxiliary.try_create_directory(file_path)
        with open(f"{file_path}.csv", "a") as file_descriptor:
            writer = csv.writer(file_descriptor, delimiter=self.DELIMITER)
            writer.writerows(object)

    def deserialize(self, file_path):
        with open(f"{file_path}.csv", "r") as file_descriptor:
            reader = csv.reader(file_descriptor, delimiter=self.DELIMITER)
            return list(reader)
