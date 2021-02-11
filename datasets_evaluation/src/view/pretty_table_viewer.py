from prettytable import PrettyTable


class PrettyTableViewer:
    EMPTY_CELL = ''

    def __init__(self, results_to_table_rows):
        self._results_to_table_rows = results_to_table_rows

    def print_result(self, result, table_name, data_writer_interface):
        self.print_results([result], table_name, ["Value"], data_writer_interface)

    def print_results(self, results, table_name, attributes_names, data_writer_interface):
        table = PrettyTable([self.EMPTY_CELL, *attributes_names])
        table.title = table_name
        table_rows = self._results_to_table_rows.get_table_rows(results)
        table.add_rows(table_rows)
        data_writer_interface.write_all(table)
