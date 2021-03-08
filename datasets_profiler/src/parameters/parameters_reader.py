from datasets_profiler.src.parameters.parameters import Parameters


class ParametersReader:
    def __init__(self, serializer_deserializer):
        self._serializer_deserializer = serializer_deserializer

    def get_parameters(self, reader_interface):
        return [Parameters(**parameters_dict) for parameters_dict in
                self._serializer_deserializer.deserialize(reader_interface.read_all())]
