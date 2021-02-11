import sys

from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Singleton, Factory

from datasets_evaluation.src.application.application import Application
from datasets_evaluation.src.application.application_initialization import ApplicationInitialization
from datasets_evaluation.src.application.repetitive_execution import RepetitiveExecution
from datasets_evaluation.src.checkpointers.persistent_checkpointer import PersistentCheckpointer
from datasets_evaluation.src.checkpointers.workflow_breaker_checkpointer import StatefulWorkflowBreakerCheckpointer
from datasets_evaluation.src.configuration.processors_operations_flags import ProcessorsOperationsFlags
from datasets_evaluation.src.configuration.spark_configuration import SparkConfiguration
from datasets_evaluation.src.dispatchers.field_dispatcher import FieldDispatcher
from datasets_evaluation.src.dispatchers.row_dispatcher import RowDispatcher
from datasets_evaluation.src.instrumentation.call_tracker import StatefulCallTracker
from datasets_evaluation.src.interfaces.readers.cli_reader import CLIReader
from datasets_evaluation.src.interfaces.readers.file_reader import FileReader
from datasets_evaluation.src.interfaces.writers.cli_writer import CLIWriter
from datasets_evaluation.src.interfaces.writers.file_writer import FileWriter
from datasets_evaluation.src.parameters.parameters_reader import ParametersReader
from datasets_evaluation.src.parsers.android_log_parser_strategy import AndroidLogParserStrategy
from datasets_evaluation.src.parsers.bgl_log_parser_strategy import BGLLogParserStrategy
from datasets_evaluation.src.parsers.edgar_log_parser_strategy import EdgarLogParserStrategy
from datasets_evaluation.src.parsers.hdfs1_log_parser_strategy import HDFS1LogParserStrategy
from datasets_evaluation.src.parsers.hdfs2_log_parser_strategy import HDFS2LogParserStrategy
from datasets_evaluation.src.parsers.mooc_log_parser_strategy import MoocLogParserStrategy
from datasets_evaluation.src.parsers.obama_visitor_log_parser_strategy import ObamaVisitorLogParserStrategy
from datasets_evaluation.src.parsers.parser import Parser
from datasets_evaluation.src.parsers.parser_commons import ParserCommons
from datasets_evaluation.src.parsers.seattle_coban_log_parser_strategy import SeattleCobanLogParserStreategy
from datasets_evaluation.src.parsers.sowiport_recommender_log_parser_strategy import SowiportRecommenderLogParserStrategy
from datasets_evaluation.src.parsers.spark_log_parser_strategy import SparkLogParserStrategy
from datasets_evaluation.src.parsers.taobao_click_log_parser_strategy import TaobaoClickLogParserStrategy
from datasets_evaluation.src.parsers.test_log_parser_strategy import TestLogParserStrategy
from datasets_evaluation.src.parsers.thunderbird_log_parser_strategy import ThunderbirdLogParserStrategy
from datasets_evaluation.src.parsers.ubuntu_dialogue_log_parser_strategy import UbuntuDialogueLogParserStrategy
from datasets_evaluation.src.parsers.user_log_parser_strategy import UserLogParserStrategy
from datasets_evaluation.src.parsers.windows_log_parser_strategy import WindowsLogParserStrategy
from datasets_evaluation.src.processors.column_statistics_calculator import ColumnStatisticsCalculator
from datasets_evaluation.src.processors.numeric_processor import NumericProcessor
from datasets_evaluation.src.processors.string_processor import StringProcessor
from datasets_evaluation.src.processors.timestamp_processor import TimestampProcessor
from datasets_evaluation.src.processors.tuple_processor import TupleProcessor
from datasets_evaluation.src.rdd_readers.rdd_text_reader import RDDTextReader
from datasets_evaluation.src.results_formatters.formatters.default_formatter import DefaultFormatter
from datasets_evaluation.src.results_formatters.formatters.no_year_datetime_formatter import NoYearDatetimeFormatter
from datasets_evaluation.src.results_formatters.results_formatter import DictionaryFormatter, ResultsFormatter
from datasets_evaluation.src.serializer_deserializer.json_serializer_deserializer import JsonSerializerDeserializer
from datasets_evaluation.src.view.csviewer import CSViewer
from datasets_evaluation.src.view.pretty_table_viewer import PrettyTableViewer
from datasets_evaluation.src.view.results_to_table_rows import ResultsToTableRows


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
    taobao_click_log_parser_strategy = Singleton(TaobaoClickLogParserStrategy, ParserCommonsProviders.parser_commons())
    mooc_log_parser_strategy = Singleton(MoocLogParserStrategy, ParserCommonsProviders.parser_commons())
    obama_visitor_log_parser_strategy = Singleton(ObamaVisitorLogParserStrategy, ParserCommonsProviders.parser_commons())
    sowiport_recommender_log_parser_strategy = Singleton(SowiportRecommenderLogParserStrategy, ParserCommonsProviders.parser_commons())
    seattle_coban_log_parser_strategy = Singleton(SeattleCobanLogParserStreategy, ParserCommonsProviders.parser_commons())
    ubuntu_dialogue_log_parser_strategy = Singleton(UbuntuDialogueLogParserStrategy, ParserCommonsProviders.parser_commons())
    user_log_parser_strategy = Singleton(UserLogParserStrategy, ParserCommonsProviders.parser_commons())


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


class SparkConfigurationProviders(DeclarativeContainer):
    spark_configuration = Singleton(SparkConfiguration)


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
                                    ProcessorsOperationsFlagsProviders.processors_operations_flags(),
                                    SparkConfigurationProviders.spark_configuration())

    tuple_processor = Singleton(TupleProcessor,
                                ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                CallTrackerProviders.stateful_call_tracker(),
                                ProcessorsOperationsFlagsProviders.processors_operations_flags())


class RDDReaderProviders(DeclarativeContainer):
    rdd_text_reader = Singleton(RDDTextReader)


class SerializerDeserializerProviders(DeclarativeContainer):
    json_serializer_deserializer = Singleton(JsonSerializerDeserializer)


class ParametersReaderProviders(DeclarativeContainer):
    parameters_reader_providers = Singleton(ParametersReader,
                                            SerializerDeserializerProviders.json_serializer_deserializer())


class InterfaceProviders(DeclarativeContainer):
    control_reader_interface = Factory(CLIReader, sys.stdin)
    control_writer_interface = Factory(CLIWriter, sys.stderr)
    data_reader_interface = Factory(FileReader)
    data_writer_interface = Factory(FileWriter)
    # data_writer_interface = Factory(CLIWriter, sys.stdout)


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
    csv_viewer = Singleton(CSViewer, ResultsToTableRowsProviders.results_to_table_rows())
    pretty_table_viewer = Singleton(PrettyTableViewer, ResultsToTableRowsProviders.results_to_table_rows())


class DictionaryFormatterProviders(DeclarativeContainer):
    dictionary_formatter = Singleton(DictionaryFormatter)


class ResultsFormatterProviders(DeclarativeContainer):
    results_formatter = Singleton(ResultsFormatter, DictionaryFormatterProviders.dictionary_formatter())


class CheckpointerProviders(DeclarativeContainer):
    persistent_checkpointer = Singleton(PersistentCheckpointer, CallTrackerProviders.stateful_call_tracker())
    stateful_workflow_breaker_checkpointer = Singleton(StatefulWorkflowBreakerCheckpointer,
                                                       SparkConfigurationProviders.spark_configuration(),
                                                       CallTrackerProviders.stateful_call_tracker())


class ApplicationInitializationProviders(DeclarativeContainer):
    application_initialization = Singleton(ApplicationInitialization,
                                           spark_configuration=SparkConfigurationProviders.spark_configuration(),
                                           rdd_reader=RDDReaderProviders.rdd_text_reader(),
                                           row_dispatcher=RowDispatcherProviders.row_dispatcher(),
                                           tuple_processor=ProcessorsProviders.tuple_processor(),
                                           column_statistics_calculator=ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                           results_viewer=ResultsViewerProviders.csv_viewer(),
                                           parser_providers=ParserProviders,
                                           formatter_providers=FormatterProviders,
                                           results_formatter=ResultsFormatterProviders.results_formatter(),
                                           checkpointer=CheckpointerProviders.stateful_workflow_breaker_checkpointer(),
                                           interface_providers=InterfaceProviders)


class ApplicationProviders(DeclarativeContainer):
    application = Singleton(Application,
                            ApplicationInitializationProviders.application_initialization(),
                            CallTrackerProviders.stateful_call_tracker())


class RepetitiveExecutionProviders(DeclarativeContainer):
    repetitive_execution = Singleton(RepetitiveExecution,
                                     ApplicationProviders.application(),
                                     ParametersReaderProviders.parameters_reader_providers(),
                                     InterfaceProviders)
