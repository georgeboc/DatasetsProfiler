import json


class JsonSerializerDeserializer:
    def serialize(self, object):
        return json.dumps(object, indent=4, sort_keys=True)

    def deserialize(self, string):
        if not string:
            return []
        return json.loads(string)
