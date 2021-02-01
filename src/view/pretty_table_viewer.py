from prettytable import PrettyTable


class PrettyTableViewer:
    def __init__(self, results_to_table_rows, interface):
        self._results_to_table_rows = results_to_table_rows
        self._interface = interface

    def print_result(self, result, table_name):
        self.print_results([result], table_name, list())

    def print_results(self, results, table_name, attributes_names):
        table = PrettyTable([table_name, *attributes_names])
        table_rows = self._results_to_table_rows.get_table_rows(results)
        table.add_rows(table_rows)
        self._interface.print_string(table)
