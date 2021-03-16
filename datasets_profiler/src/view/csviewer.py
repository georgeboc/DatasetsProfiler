class CSViewer:
    BREAK_LINE = '\n'
    EMPTY_CELL = ''

    def __init__(self, results_to_table_rows, serializer_deserializer):
        self._results_to_table_rows = results_to_table_rows
        self._serializer_deserializer = serializer_deserializer

    def print_result(self, result, table_name, file_path):
        self.print_results([result], table_name, ["Value"], file_path)

    def print_results(self, results, table_name, attributes_names, file_path):
        table = []
        table.append([table_name])
        table.append([self.EMPTY_CELL, *attributes_names])
        table_rows = self._results_to_table_rows.get_table_rows(results)
        table.extend(([str(element) for element in table_row] for table_row in table_rows))
        table.append([])
        self._serializer_deserializer.serialize(table, file_path)