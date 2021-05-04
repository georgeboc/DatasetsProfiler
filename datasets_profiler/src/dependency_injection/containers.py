from argparse import ArgumentParser

from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Singleton

from datasets_profiler.src.application.application import Application
from datasets_profiler.src.application.repetitive_execution import RepetitiveExecution
from datasets_profiler.src.checkers.local_execution_checker import LocalExecutionChecker
from datasets_profiler.src.checkpointers.persistent_checkpointer import PersistentCheckpointer
from datasets_profiler.src.checkpointers.workflow_breaker_checkpointer import WorkflowBreakerCheckpointer
from datasets_profiler.src.configuration.processors_operations_flags import ProcessorsOperationsFlags
from datasets_profiler.src.configuration.spark_configuration import SparkConfiguration
from datasets_profiler.src.dispatchers.one_processor_dispatcher import OneProcessorDispatcher
from datasets_profiler.src.dispatchers.type_dispatcher import TypeDispatcher
from datasets_profiler.src.dispatchers.column_dispatcher import ColumnDispatcher
from datasets_profiler.src.filesystems.directories_auxiliary import DirectoriesAuxiliary
from datasets_profiler.src.filesystems.hdfs_filesystem import HDFSFileSystem
from datasets_profiler.src.filesystems.local_filesystem import LocalFileSystem
from datasets_profiler.src.filesystems.proxy_filesystem import ProxyFilesystem
from datasets_profiler.src.instrumentation.call_tracker import StatefulCallTracker
from datasets_profiler.src.arguments_parser.arguments_parser import ArgumentsParser
from datasets_profiler.src.logs.log_initializer import LogInitializer
from datasets_profiler.src.parameters.get_described_dataset_parameters import GetDescribedDatasetParameters
from datasets_profiler.src.parameters.get_description_parameters import GetDescriptionParameters
from datasets_profiler.src.parameters.parameters_reader import ParametersReader
from datasets_profiler.src.parsers.android_log_parser_strategy import AndroidLogParserStrategy
from datasets_profiler.src.parsers.bgl_log_parser_strategy import BGLLogParserStrategy
from datasets_profiler.src.parsers.edgar_log_parser_strategy import EdgarLogParserStrategy
from datasets_profiler.src.parsers.hdfs1_log_parser_strategy import HDFS1LogParserStrategy
from datasets_profiler.src.parsers.hdfs2_log_parser_strategy import HDFS2LogParserStrategy
from datasets_profiler.src.parsers.mooc_log_parser_strategy import MoocLogParserStrategy
from datasets_profiler.src.parsers.obama_visitor_log_parser_strategy import ObamaVisitorLogParserStrategy
from datasets_profiler.src.parsers.parser import Parser
from datasets_profiler.src.parsers.parser_commons import ParserCommons
from datasets_profiler.src.parsers.seattle_coban_log_parser_strategy import SeattleCobanLogParserStreategy
from datasets_profiler.src.parsers.recommender_click_logs_sowiport_log_parser_strategy import RecommenderClickLogsSowiportLogParserStrategy
from datasets_profiler.src.parsers.spark_log_parser_strategy import SparkLogParserStrategy
from datasets_profiler.src.parsers.ad_click_on_taobao_log_parser_strategy import AdClickOnTaobaoLogParserStrategy
from datasets_profiler.src.parsers.test_log_parser_strategy import TestLogParserStrategy
from datasets_profiler.src.parsers.thunderbird_log_parser_strategy import ThunderbirdLogParserStrategy
from datasets_profiler.src.parsers.ubuntu_dialogue_corpus_log_parser_strategy import UbuntuDialogueCorpusLogParserStrategy
from datasets_profiler.src.parsers.user_logs_v2_log_parser_strategy import UserLogsV2LogParserStrategy
from datasets_profiler.src.parsers.windows_log_parser_strategy import WindowsLogParserStrategy
from datasets_profiler.src.processors.column_statistics_calculator import ColumnStatisticsCalculator
from datasets_profiler.src.processors.count_values_processor import CountValuesProcessor
from datasets_profiler.src.processors.numeric_processor import NumericProcessor
from datasets_profiler.src.processors.string_processor import StringProcessor
from datasets_profiler.src.processors.timestamp_processor import TimestampProcessor
from datasets_profiler.src.processors.tuple_processor import TupleProcessor
from datasets_profiler.src.rdd_readers.rdd_text_reader import RDDTextReader
from datasets_profiler.src.formatters.specific_formatters.string_specific_formatter import StringSpecificFormatter
from datasets_profiler.src.formatters.specific_formatters.no_year_datetime_specific_formatter import NoYearDatetimeSpecificFormatter
from datasets_profiler.src.formatters.formatter import DictionaryFormatter, Formatter
from datasets_profiler.src.serializers_deserializers.avro_dataframe_serializer_deserializer import AvroDataFrameSerializerDeserializer
from datasets_profiler.src.serializers_deserializers.csv_serializer_deserializer import CSVSerializerDeserializer
from datasets_profiler.src.serializers_deserializers.json_serializer_deserializer import JsonSerializerDeserializer
from datasets_profiler.src.serializers_deserializers.parquet_dataframe_serializer_deserializer import \
    ParquetDataframeSerializerDeserializer
from datasets_profiler.src.stringifier.data_frame_stringifier import DataFrameStringifier
from datasets_profiler.src.use_cases.get_described_dataset import GetDescribedDataset, GetDescribedDatasetInitialization
from datasets_profiler.src.use_cases.get_description import GetDescription, GetDescriptionInitialization
from datasets_profiler.src.viewers.avro_viewer import AvroViewer
from datasets_profiler.src.viewers.csv_viewer import CSVViewer
from datasets_profiler.src.viewers.pretty_table_viewer import PrettyTableViewer
from datasets_profiler.src.viewers.results_to_table_rows import ResultsToTableRows


from pyhdfs import HdfsClient


class ParserCommonsProviders(DeclarativeContainer):
    parser_commons = Singleton(ParserCommons)


class ParserStrategyProviders(DeclarativeContainer):
    android_log_parser_strategy = Singleton(AndroidLogParserStrategy,
                                            ParserCommonsProviders.parser_commons())
    bgl_log_parser_strategy = Singleton(BGLLogParserStrategy,
                                        ParserCommonsProviders.parser_commons())
    edgar_log_parser_strategy = Singleton(EdgarLogParserStrategy,
                                          ParserCommonsProviders.parser_commons())
    hdfs1_log_parser_strategy = Singleton(HDFS1LogParserStrategy,
                                          ParserCommonsProviders.parser_commons())
    hdfs2_log_parser_strategy = Singleton(HDFS2LogParserStrategy,
                                          ParserCommonsProviders.parser_commons())
    spark_log_parser_strategy = Singleton(SparkLogParserStrategy,
                                          ParserCommonsProviders.parser_commons())
    test_log_parser_strategy = Singleton(TestLogParserStrategy,
                                         ParserCommonsProviders.parser_commons())
    thunderbird_log_parser_strategy = Singleton(ThunderbirdLogParserStrategy,
                                                ParserCommonsProviders.parser_commons())
    windows_log_parser_strategy = Singleton(WindowsLogParserStrategy,
                                            ParserCommonsProviders.parser_commons())
    ad_click_on_taobao_log_parser_strategy = Singleton(AdClickOnTaobaoLogParserStrategy,
                                                       ParserCommonsProviders.parser_commons())
    mooc_log_parser_strategy = Singleton(MoocLogParserStrategy,
                                         ParserCommonsProviders.parser_commons())
    obama_visitor_log_parser_strategy = Singleton(ObamaVisitorLogParserStrategy,
                                                  ParserCommonsProviders.parser_commons())
    recommender_click_logs_sowiport_log_parser_strategy = Singleton(RecommenderClickLogsSowiportLogParserStrategy,
                                                                    ParserCommonsProviders.parser_commons())
    seattle_coban_log_parser_strategy = Singleton(SeattleCobanLogParserStreategy,
                                                  ParserCommonsProviders.parser_commons())
    ubuntu_dialogue_corpus_log_parser_strategy = Singleton(UbuntuDialogueCorpusLogParserStrategy,
                                                           ParserCommonsProviders.parser_commons())
    user_logs_v2_log_parser_strategy = Singleton(UserLogsV2LogParserStrategy,
                                                 ParserCommonsProviders.parser_commons())


class ParserProviders(DeclarativeContainer):
    @staticmethod
    def get_parser_by_name(parser_name):
        return Singleton(Parser, ParserStrategyProviders.providers[parser_name](),
                         SparkConfigurationProviders.spark_configuration())()


class CallTrackerProviders(DeclarativeContainer):
    stateful_call_tracker = Singleton(StatefulCallTracker)


class SpecificFormatterProviders(DeclarativeContainer):
    string_specific_formatter = Singleton(StringSpecificFormatter)
    no_year_datetime_specific_formatter = Singleton(NoYearDatetimeSpecificFormatter)

    @classmethod
    def get_specific_formatter_by_name(cls, specific_formatter_name):
        return cls.providers[specific_formatter_name]()


class ProcessorsOperationsFlagsProviders(DeclarativeContainer):
    processors_operations_flags = Singleton(ProcessorsOperationsFlags)


class ColumnStatisticsCalculatorProviders(DeclarativeContainer):
    column_statistics_calculator = Singleton(ColumnStatisticsCalculator,
                                             CallTrackerProviders.stateful_call_tracker(),
                                             ProcessorsOperationsFlagsProviders.processors_operations_flags())


class ArgumentsParserProviders(DeclarativeContainer):
    arguments_parser = Singleton(ArgumentsParser, ArgumentParser())


class LocalExecutionCheckerProvider(DeclarativeContainer):
    local_execution_checker = Singleton(LocalExecutionChecker, ArgumentsParserProviders.arguments_parser())


class SparkConfigurationProviders(DeclarativeContainer):
    spark_configuration = Singleton(SparkConfiguration, LocalExecutionCheckerProvider.local_execution_checker())


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

    count_values_processor = Singleton (CountValuesProcessor,
                                        ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                        SparkConfigurationProviders.spark_configuration(),
                                        CallTrackerProviders.stateful_call_tracker())


class RDDReaderProviders(DeclarativeContainer):
    rdd_text_reader = Singleton(RDDTextReader, SparkConfigurationProviders.spark_configuration())


class FilesystemProviders(DeclarativeContainer):
    USER_NAME = "bochileanu"

    local_filesystem = Singleton(LocalFileSystem)
    hdfs_filesystem = Singleton(HDFSFileSystem, HdfsClient(hosts='dtim:9870', user_name=USER_NAME))
    proxy_filesystem = Singleton(ProxyFilesystem, local_filesystem, hdfs_filesystem, LocalExecutionCheckerProvider.local_execution_checker())


class DirectoriesAuxiliaryProviders(DeclarativeContainer):
    directories_auxiliary = Singleton(DirectoriesAuxiliary, FilesystemProviders.proxy_filesystem())


class SerializerDeserializerProviders(DeclarativeContainer):
    json_serializer_deserializer = Singleton(JsonSerializerDeserializer, DirectoriesAuxiliaryProviders.directories_auxiliary(), FilesystemProviders.proxy_filesystem())
    csv_serializer_deserializer = Singleton(CSVSerializerDeserializer, DirectoriesAuxiliaryProviders.directories_auxiliary(), FilesystemProviders.proxy_filesystem())

    avro_dataframe_serializer_deserializer = Singleton(AvroDataFrameSerializerDeserializer,
                                                       SparkConfigurationProviders.spark_configuration(),
                                                       DirectoriesAuxiliaryProviders.directories_auxiliary(),
                                                       FilesystemProviders.proxy_filesystem())
    parquet_dataframe_serializer_deserializer = Singleton(ParquetDataframeSerializerDeserializer,
                                                          SparkConfigurationProviders.spark_configuration(),
                                                          DirectoriesAuxiliaryProviders.directories_auxiliary())


class ParametersClassesProviders(DeclarativeContainer):
    parameters_classes = Singleton(dict, {
        "get_described_dataset": GetDescribedDatasetParameters,
        "get_description": GetDescriptionParameters
    })


class ParametersReaderProviders(DeclarativeContainer):
    parameters_reader_providers = Singleton(ParametersReader,
                                            ParametersClassesProviders.parameters_classes(),
                                            SerializerDeserializerProviders.json_serializer_deserializer())


class TypeProcessorsProviders(DeclarativeContainer):
    type_processors = Singleton(dict, {
        "int": ProcessorsProviders.numeric_processor(),
        "long": ProcessorsProviders.numeric_processor(),
        "bigint": ProcessorsProviders.numeric_processor(),
        "float": ProcessorsProviders.numeric_processor(),
        "string": ProcessorsProviders.string_processor(),
        "timestamp": ProcessorsProviders.timestamp_processor()
    })


class DispatcherProviders(DeclarativeContainer):
    type_dispatcher = Singleton(TypeDispatcher,
                                TypeProcessorsProviders.type_processors(),
                                SparkConfigurationProviders.spark_configuration())
    one_processor_dispatcher = Singleton(OneProcessorDispatcher,
                                         ProcessorsProviders.count_values_processor())


class ColumnDispatcherProviders(DeclarativeContainer):
    type_column_dispatcher = Singleton(ColumnDispatcher, DispatcherProviders.type_dispatcher())
    one_processor_column_dispatcher = Singleton(ColumnDispatcher, DispatcherProviders.one_processor_dispatcher())


class ResultsToTableRowsProviders(DeclarativeContainer):
    results_to_table_rows = Singleton(ResultsToTableRows)


class ViewerProviders(DeclarativeContainer):
    csv_viewer = Singleton(CSVViewer, ResultsToTableRowsProviders.results_to_table_rows(),
                           SerializerDeserializerProviders.csv_serializer_deserializer())
    pretty_table_viewer = Singleton(PrettyTableViewer, ResultsToTableRowsProviders.results_to_table_rows())
    avro_viewer = Singleton(AvroViewer, ResultsToTableRowsProviders.results_to_table_rows(),
                            SerializerDeserializerProviders.avro_dataframe_serializer_deserializer())


class DictionaryFormatterProviders(DeclarativeContainer):
    dictionary_formatter = Singleton(DictionaryFormatter)


class FormatterProviders(DeclarativeContainer):
    formatter = Singleton(Formatter, DictionaryFormatterProviders.dictionary_formatter())


class CheckpointerProviders(DeclarativeContainer):
    persistent_checkpointer = Singleton(PersistentCheckpointer,
                                        CallTrackerProviders.stateful_call_tracker())
    workflow_breaker_checkpointer = Singleton(WorkflowBreakerCheckpointer,
                                              SerializerDeserializerProviders.parquet_dataframe_serializer_deserializer(),
                                              FilesystemProviders.proxy_filesystem(),
                                              CallTrackerProviders.stateful_call_tracker())


class LogProviders(DeclarativeContainer):
    log_initializer = Singleton(LogInitializer, FilesystemProviders.local_filesystem())


class StringifierProviders(DeclarativeContainer):
    data_frame_stringifier = Singleton(DataFrameStringifier)


class InitializationProviders(DeclarativeContainer):
    get_described_dataset_initialization = Singleton(GetDescribedDatasetInitialization,
                                                     spark_configuration=SparkConfigurationProviders.spark_configuration(),
                                                     rdd_reader=RDDReaderProviders.rdd_text_reader(),
                                                     column_dispatcher=ColumnDispatcherProviders.one_processor_column_dispatcher(),
                                                     parser_providers=ParserProviders,
                                                     checkpointer=CheckpointerProviders.workflow_breaker_checkpointer(),
                                                     formatter=FormatterProviders.formatter(),
                                                     viewer=ViewerProviders.avro_viewer(),
                                                     data_frame_stringifier=StringifierProviders.data_frame_stringifier(),
                                                     data_frame_serializer=SerializerDeserializerProviders.parquet_dataframe_serializer_deserializer()
                                                     )
    get_description_initialization = Singleton(GetDescriptionInitialization,
                                               spark_configuration=SparkConfigurationProviders.spark_configuration(),
                                               rdd_reader=RDDReaderProviders.rdd_text_reader(),
                                               column_dispatcher=ColumnDispatcherProviders.type_column_dispatcher(),
                                               tuple_processor=ProcessorsProviders.tuple_processor(),
                                               column_statistics_calculator=ColumnStatisticsCalculatorProviders.column_statistics_calculator(),
                                               viewer=ViewerProviders.csv_viewer(),
                                               parser_providers=ParserProviders,
                                               specific_formatters_providers=SpecificFormatterProviders,
                                               formatter=FormatterProviders.formatter(),
                                               checkpointer=CheckpointerProviders.workflow_breaker_checkpointer())


class UseCaseProviders(DeclarativeContainer):
    get_description = Singleton(GetDescription,
                                InitializationProviders.get_description_initialization(),
                                CallTrackerProviders.stateful_call_tracker())
    get_described_dataset = Singleton(GetDescribedDataset,
                                      InitializationProviders.get_described_dataset_initialization())

    @classmethod
    def get_use_case_by_name(cls, use_case_name):
        return cls.providers[use_case_name]()


class ApplicationProviders(DeclarativeContainer):
    application = Singleton(Application, UseCaseProviders)


class RepetitiveExecutionProviders(DeclarativeContainer):
    repetitive_execution = Singleton(RepetitiveExecution,
                                     ApplicationProviders.application(),
                                     ParametersReaderProviders.parameters_reader_providers(),
                                     LogProviders.log_initializer(),
                                     ArgumentsParserProviders.arguments_parser())
