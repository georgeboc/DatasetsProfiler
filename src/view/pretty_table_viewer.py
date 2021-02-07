from prettytable import PrettyTable


class PrettyTableViewer:
    EMPTY_CELL = ''

    def __init__(self, results_to_table_rows, interface):
        self._results_to_table_rows = results_to_table_rows
        self._interface = interface

    def print_result(self, result, table_name):
        self.print_results([result], table_name, ["Value"])

    def print_results(self, results, table_name, attributes_names):
        table = PrettyTable([self.EMPTY_CELL, *attributes_names])
        table.title = table_name
        table_rows = self._results_to_table_rows.get_table_rows(results)
        table.add_rows(table_rows)
        self._interface.print_string(table)
