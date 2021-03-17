import json

from datasets_profiler.src.serializers_deserializers.directories_auxiliary import try_create_directory


class JsonSerializerDeserializer:
    def serialize(self, object, file_path):
        try_create_directory(file_path)
        with open(f"{file_path}.json", "w+") as file_descriptor:
            return json.dump(object, file_descriptor, indent=4, sort_keys=True)

    def deserialize(self, file_path):
        with open(f"{file_path}.json", "r") as file_descriptor:
            return json.load(file_descriptor)
