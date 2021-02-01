class CSViewer:
    SEPARATOR = ';'

    def __init__(self, results_to_table_rows):
        self._results_to_table_rows = results_to_table_rows

    def print_results(self, results, attributes_names):
        table = self.SEPARATOR.join(['', *attributes_names]) + '\n'
        table_rows = list(self._results_to_table_rows.get_table_rows(results))
        for table_row in table_rows:
            table += self.SEPARATOR.join(str(element) for element in table_row) + "\n"
        print(table)