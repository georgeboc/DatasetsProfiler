class CSViewer:
    SEPARATOR = ';'

    def __init__(self, results_to_table_rows, interface):
        self._results_to_table_rows = results_to_table_rows
        self._interface = interface

    def print_result(self, result, table_name):
        self.print_results([result], table_name, list())

    def print_results(self, results, table_name, attributes_names):
        table = self.SEPARATOR.join([table_name, *attributes_names]) + '\n'
        table_rows = list(self._results_to_table_rows.get_table_rows(results))
        for table_row in table_rows:
            table += self.SEPARATOR.join(str(element) for element in table_row) + "\n"
        self._interface.print_string(table)