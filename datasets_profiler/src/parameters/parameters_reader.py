from datasets_profiler.src.parameters.parameters import Parameters


class ParametersReader:
    def __init__(self, serializer_deserializer):
        self._serializer_deserializer = serializer_deserializer

    def get_parameters(self, parameters_file_path):
        return [Parameters(**parameters_dict) for parameters_dict in
                self._serializer_deserializer.deserialize(parameters_file_path)]
