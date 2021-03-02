class CSViewer:
    SEPARATOR = ';'
    BREAK_LINE = '\n'
    EMPTY_CELL = ''

    def __init__(self, results_to_table_rows):
        self._results_to_table_rows = results_to_table_rows

    def print_result(self, result, table_name, data_writer_interface):
        self.print_results([result], table_name, ["Value"], data_writer_interface)

    def print_results(self, results, table_name, attributes_names, data_writer_interface):
        table = table_name + self.BREAK_LINE
        table += self.SEPARATOR.join([self.EMPTY_CELL, *attributes_names]) + self.BREAK_LINE
        table_rows = self._results_to_table_rows.get_table_rows(results)
        for table_row in table_rows:
            table += self.SEPARATOR.join(str(element) for element in table_row) + self.BREAK_LINE
        table += self.BREAK_LINE
        data_writer_interface.write_all(table)