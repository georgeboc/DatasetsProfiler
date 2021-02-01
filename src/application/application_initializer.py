from application.application import Application
from processors.column_statistics_calculator import ColumnStatisticsCalculator
from dispatchers.field_dispatcher import FieldDispatcher
from dispatchers.row_dispatcher import RowDispatcher
from processors.integer_processor import IntegerProcessor
from processors.string_processor import StringProcessor
from processors.timestamp_processor import TimestampProcessor
from readers.csv_reader import CSVReader
from schema_transformers.log_schema_transformer import LogDataFrameTransformer


class ApplicationInitializer:
    def initialize(self):
        reader = CSVReader()

        data_frame_transformer = LogDataFrameTransformer()

        column_statistics_calculator = ColumnStatisticsCalculator()
        integer_processor = IntegerProcessor(column_statistics_calculator)
        string_processor = StringProcessor(column_statistics_calculator)
        timestamp_processor = TimestampProcessor(column_statistics_calculator)
        type_processors = {
            "IntegerType": integer_processor,
            "FloatType": integer_processor,
            "LongType": integer_processor,
            "StringType": string_processor,
            "TimestampType": timestamp_processor
        }
        field_dispatcher = FieldDispatcher(type_processors)
        row_dispatcher = RowDispatcher(field_dispatcher)

        return Application(reader, data_frame_transformer, row_dispatcher, column_statistics_calculator)