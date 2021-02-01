class FieldDispatcher:
    WITHOUT_REPLACEMENT = "false"

    def __init__(self, type_processors):
        self._type_processors = type_processors

    def dispatch(self, column_rdd, column_data_type):
        return self._type_processors[column_data_type].process(column_rdd)
