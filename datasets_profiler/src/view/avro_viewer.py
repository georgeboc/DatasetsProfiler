class AvroViewer:
    def __init__(self, results_to_table_rows, serializer_deserializer):
        self._results_to_table_rows = results_to_table_rows
        self._serializer_deserializer = serializer_deserializer

    def view(self, results, table_name, file_path):
        for result in results:
            self._serializer_deserializer.serialize(result.dictionary, self._get_path(file_path,
                                                                                      table_name,
                                                                                      result.column_name))

    def _get_path(self, file_path, table_name, column_name):
        return f"{file_path}/{table_name}_{column_name}"
