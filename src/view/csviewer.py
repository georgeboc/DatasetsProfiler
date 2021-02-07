class CSViewer:
    SEPARATOR = ';'
    BREAK_LINE = '\n'
    EMPTY_CELL = ''

    def __init__(self, results_to_table_rows, interface):
        self._results_to_table_rows = results_to_table_rows
        self._interface = interface

    def print_result(self, result, table_name):
        self.print_results([result], table_name, ["Value"])

    def print_results(self, results, table_name, attributes_names):
        table = table_name + self.BREAK_LINE
        table += self.SEPARATOR.join([self.EMPTY_CELL, *attributes_names]) + self.BREAK_LINE
        table_rows = self._results_to_table_rows.get_table_rows(results)
        for table_row in table_rows:
            table += self.SEPARATOR.join(str(element) for element in table_row) + self.BREAK_LINE
        self._interface.print_string(table)