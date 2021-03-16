import json


class JsonSerializerDeserializer:
    def serialize(self, object, file_path):
        with open(file_path, "w+") as file_descriptor:
            return json.dump(object, file_descriptor, indent=4, sort_keys=True)

    def deserialize(self, file_path):
        with open(file_path, "r") as file_descriptor:
            return json.load(file_descriptor)
