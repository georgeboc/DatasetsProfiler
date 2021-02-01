from application.application import Application
from data_frame_transformers.log_data_frame_transformer_with_integer import LogDataFrameTransformerWithInteger
from dispatchers.data_frame_transformer_dispatcher import DataFrameTransformerDispatcher
from interfaces.command_line_interface import CommandLineInterface
from processors.column_statistics_calculator import ColumnStatisticsCalculator
from dispatchers.field_dispatcher import FieldDispatcher
from dispatchers.row_dispatcher import RowDispatcher
from processors.integer_processor import IntegerProcessor
from processors.string_processor import StringProcessor
from processors.timestamp_processor import TimestampProcessor
from readers.csv_reader import CSVReader
from data_frame_transformers.log_data_frame_transformer import LogDataFrameTransformer
from view.csviewer import CSViewer
from view.pretty_table_viewer import PrettyTableViewer
from view.results_to_table_rows import ResultsToTableRows


class ApplicationInitializer:
    def initialize(self):
        reader = CSVReader()

        command_line_interface = CommandLineInterface()

        data_frame_transformer_classes = {
            "LogDataFrameTransformer": LogDataFrameTransformer,
            "LogDataFrameTransformerWithInteger": LogDataFrameTransformerWithInteger
        }
        data_frame_transformer_dispatcher = DataFrameTransformerDispatcher(data_frame_transformer_classes)

        column_statistics_calculator = ColumnStatisticsCalculator()
        integer_processor = IntegerProcessor(column_statistics_calculator)
        string_processor = StringProcessor(column_statistics_calculator)
        timestamp_processor = TimestampProcessor(column_statistics_calculator)
        type_processors = {
            "bigint": integer_processor,
            "string": string_processor,
            "timestamp": timestamp_processor
        }
        field_dispatcher = FieldDispatcher(type_processors)
        row_dispatcher = RowDispatcher(field_dispatcher)

        results_to_table_rows = ResultsToTableRows()
        results_viewer = CSViewer(results_to_table_rows)
        return Application(reader, row_dispatcher, column_statistics_calculator, results_viewer,
                           command_line_interface, data_frame_transformer_dispatcher)