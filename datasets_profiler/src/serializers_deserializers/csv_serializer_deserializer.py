import csv


class CSVSerializerDeserializer:
    DELIMITER = ";"

    def serialize(self, object, file_path):
        with open(file_path, "a") as file_descriptor:
            writer = csv.writer(file_descriptor, delimiter=self.DELIMITER)
            writer.writerows(object)

    def deserialize(self, file_path):
        with open(file_path, "r") as file_descriptor:
            reader = csv.reader(file_descriptor, delimiter=self.DELIMITER)
            return list(reader)
