import json


class JsonSerializerDeserializer:
    UTF8 = "utf-8"

    def __init__(self, directories_auxiliary, filesystem):
        self._directories_auxiliary = directories_auxiliary
        self._filesystem = filesystem

    def serialize(self, object, file_path):
        self._directories_auxiliary.try_create_directory(file_path)
        self._filesystem.write_string(json.dumps(object, indent=4, sort_keys=True), file_path)

    def deserialize(self, file_path):
        return json.loads(self._filesystem.read(file_path).decode(self.UTF8))
