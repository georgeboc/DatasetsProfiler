class ParametersReader:
    USE_CASE = "use_case"

    def __init__(self, parameters_classes, serializer_deserializer):
        self._parameters_classes = parameters_classes
        self._serializer_deserializer = serializer_deserializer

    def get_parameters(self, parameters_file_path):
        return [self._parameters_classes[parameters_dict[self.USE_CASE]](**parameters_dict)
                for parameters_dict in self._serializer_deserializer.deserialize(parameters_file_path)]
