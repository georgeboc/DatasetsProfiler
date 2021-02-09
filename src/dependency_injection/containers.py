from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Singleton

from application.application import Application
from application.application_initialization import ApplicationInitialization
from configuration.processors_operations_flags import ProcessorsOperationsFlags
from configuration.spark_configuration import SparkConfiguration
from dispatchers.field_dispatcher import FieldDispatcher
from dispatchers.row_dispatcher import RowDispatcher
from instrumentation.call_tracker import StatefulCallTracker
from results_formatters.formatters.default_formatter import DefaultFormatter
from results_formatters.formatters.no_year_datetime_formatter import NoYearDatetimeFormatter
from results_formatters.results_formatter import DictionaryFormatter, ResultsFormatter
from parsers.android_log_parser_strategy import AndroidLogParserStrategy
from parsers.bgl_log_parser_strategy import BGLLogParserStrategy
from parsers.edgar_log_parser_strategy import EdgarLogParserStrategy
from parsers.hdfs1_log_parser_strategy import HDFS1LogParserStrategy
from parsers.hdfs2_log_parser_strategy import HDFS2LogParserStrategy
from parsers.parser import Parser
from parsers.parser_commons import ParserCommons
from parsers.spark_log_parser_strategy import SparkLogParserStrategy
from parsers.test_log_parser_strategy import TestLogParserStrategy
from parsers.thunderbird_log_parser_strategy import ThunderbirdLogParserStrategy
from parsers.windows_log_parser_strategy import WindowsLogParserStrategy
from processors.column_statistics_calculator import ColumnStatisticsCalculator
from processors.numeric_processor import NumericProcessor
from processors.string_processor import StringProcessor
from processors.timestamp_processor import TimestampProcessor
from processors.tuple_processor import TupleProcessor
from readers.text_reader import TextReader
from interfaces.command_line_interface import CommandLineInterface
from view.csviewer import CSViewer
from view.pretty_table_viewer import PrettyTableViewer
from view.results_to_table_rows import ResultsToTableRows


class ParserCommonsProviders(DeclarativeContainer):
    parser_commons = Singleton(ParserCommons)


class ParserStrategyProviders(DeclarativeContainer):
    android_log_parser_strategy = Singleton(AndroidLogParserStrategy, ParserCommonsProviders.parser_commons())
    bgl_log_parser_strategy = Singleton(BGLLogParserStrategy, ParserCommonsProviders.parser_commons())
    edgar_log_parser_strategy = Singleton(EdgarLogParserStrategy, ParserCommonsProviders.parser_commons())
    hdfs1_log_parser_strategy = Singleton(HDFS1LogParserStrategy, ParserCommonsProviders.parser_commons())
    hdfs2_log_parser_strategy = Singleton(HDFS2LogParserStrategy, ParserCommonsProviders.parser_commons())
    spark_log_parser_strategy = Singleton(SparkLogParserStrategy, ParserCommonsProviders.parser_commons())
    test_log_parser_strategy = Singleton(TestLogParserStrategy, ParserCommonsProviders.parser_commons())
    thunderbird_log_parser_strategy = Singleton(ThunderbirdLogParserStrategy, ParserCommonsProviders.parser_commons())
    windows_log_parser_strategy = Singleton(WindowsLogParserStrategy, ParserCommonsProviders.parser_commons())


class ParserProviders(DeclarativeContainer):
    @staticmethod
    def parser(parser_strategy):
        return Singleton(Parser, ParserStrategyProviders.providers[parser_strategy]())()


class CallTrackerProviders(DeclarativeContainer):
    stateful_call_tracker = Singleton(StatefulCallTracker)


class FormatterProviders(DeclarativeContainer):
    default_formatter = Singleton(DefaultFormatter)
    no_year_datetime_formatter = Singleton(NoYearDatetimeFormatter)


class ProcessorsOperationsFlagsProviders(DeclarativeContainer):
    processors_operations_flags = Singleton(ProcessorsOperationsFlags)


class ColumnStatisticsCalculatorProviders(DeclarativeContainer):
    column_statistics_calculator = Singleton(ColumnStatisticsCalculator,
                                             CallTrackerProviders.stateful_call_tracker(),
                                             ProcessorsOperationsFlagsProviders.processors_operations_flags())


class ProcessorsProviders(DeclarativeContainer):
    numeric_processor = Singleton(NumericProcessor,
                                  ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                  CallTrackerProviders.stateful_call_tracker(),
                                  ProcessorsOperationsFlagsProviders.processors_operations_flags())
    string_processor = Singleton(StringProcessor,
                                 ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                 CallTrackerProviders.stateful_call_tracker(),
                                 ProcessorsOperationsFlagsProviders.processors_operations_flags())
    timestamp_processor = Singleton(TimestampProcessor,
                                    ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                    CallTrackerProviders.stateful_call_tracker(),
                                    ProcessorsOperationsFlagsProviders.processors_operations_flags())

    tuple_processor = Singleton(TupleProcessor,
                                ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                CallTrackerProviders.stateful_call_tracker(),
                                ProcessorsOperationsFlagsProviders.processors_operations_flags())


class SparkConfigurationProviders(DeclarativeContainer):
    spark_configuration = Singleton(SparkConfiguration)


class ReaderProviders(DeclarativeContainer):
    text_reader = Singleton(TextReader)


class InterfaceProviders(DeclarativeContainer):
    command_line_interface = Singleton(CommandLineInterface)


class TypeProcessorsProviders(DeclarativeContainer):
    type_processors = Singleton(dict, {
        "int": ProcessorsProviders.numeric_processor(),
        "long": ProcessorsProviders.numeric_processor(),
        "bigint": ProcessorsProviders.numeric_processor(),
        "float": ProcessorsProviders.numeric_processor(),
        "string": ProcessorsProviders.string_processor(),
        "timestamp": ProcessorsProviders.timestamp_processor()
    })


class FieldDispatcherProviders(DeclarativeContainer):
    field_dispatcher = Singleton(FieldDispatcher, TypeProcessorsProviders.type_processors())


class RowDispatcherProviders(DeclarativeContainer):
    row_dispatcher = Singleton(RowDispatcher, FieldDispatcherProviders.field_dispatcher())


class ResultsToTableRowsProviders(DeclarativeContainer):
    results_to_table_rows = Singleton(ResultsToTableRows)


class ResultsViewerProviders(DeclarativeContainer):
    csv_viewer = Singleton(CSViewer, ResultsToTableRowsProviders.results_to_table_rows(), InterfaceProviders.command_line_interface())
    pretty_table_viewer = Singleton(PrettyTableViewer, ResultsToTableRowsProviders.results_to_table_rows(), InterfaceProviders.command_line_interface())


class DictionaryFormatterProviders(DeclarativeContainer):
    dictionary_formatter = Singleton(DictionaryFormatter)


class ResultsFormatterProviders(DeclarativeContainer):
    results_formatter = Singleton(ResultsFormatter, DictionaryFormatterProviders.dictionary_formatter())


class ApplicationInitializationProviders(DeclarativeContainer):
    application_initialization = Singleton(ApplicationInitialization,
                                           spark_configuration=SparkConfigurationProviders.spark_configuration(),
                                           reader=ReaderProviders.text_reader(),
                                           row_dispatcher=RowDispatcherProviders.row_dispatcher(),
                                           tuple_processor=ProcessorsProviders.tuple_processor(),
                                           column_statistics_calculator=ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                           results_viewer=ResultsViewerProviders.csv_viewer(),
                                           interface=InterfaceProviders.command_line_interface(),
                                           parser_providers=ParserProviders,
                                           formatter_providers=FormatterProviders,
                                           results_formatter=ResultsFormatterProviders.results_formatter())


class ApplicationProviders(DeclarativeContainer):
    application = Singleton(Application,
                            ApplicationInitializationProviders.application_initialization(),
                            CallTrackerProviders.stateful_call_tracker())
