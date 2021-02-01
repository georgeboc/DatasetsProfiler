from prettytable import PrettyTable


class PrettyTableViewer:
    def __init__(self, results_to_table_rows):
        self._results_to_table_rows = results_to_table_rows

    def print_results(self, results, attributes_names):
        table = PrettyTable(['', *attributes_names])
        table_rows = self._results_to_table_rows.get_table_rows(results)
        table.add_rows(table_rows)
        print(table)
