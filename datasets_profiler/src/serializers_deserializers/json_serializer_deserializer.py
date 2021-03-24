import json


class JsonSerializerDeserializer:
    def __init__(self, directories_auxiliary):
        self._directories_auxiliary = directories_auxiliary

    def serialize(self, object, file_path):
        self._directories_auxiliary.try_create_directory(file_path)
        with open(f"{file_path}.json", "w+") as file_descriptor:
            return json.dump(object, file_descriptor, indent=4, sort_keys=True)

    def deserialize(self, file_path):
        with open(f"{file_path}.json", "r") as file_descriptor:
            return json.load(file_descriptor)
