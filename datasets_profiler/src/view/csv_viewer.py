class CSVViewer:
    BREAK_LINE = '\n'
    EMPTY_CELL = ''

    def __init__(self, results_to_table_rows, serializer_deserializer):
        self._results_to_table_rows = results_to_table_rows
        self._serializer_deserializer = serializer_deserializer

    def view(self, results, table_name, file_path):
        table = [[table_name], [self.EMPTY_CELL, *self._results_to_table_rows.get_column_names(results)]]
        table_rows = self._results_to_table_rows.get_table_rows(results)
        table.extend(([str(element) for element in table_row] for table_row in table_rows))
        table.append([])
        self._serializer_deserializer.serialize(table, file_path)