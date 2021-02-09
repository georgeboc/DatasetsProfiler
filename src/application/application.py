from dataclasses import asdict
from pathlib import Path

from instrumentation.call_tracker import instrument_call
from results_formatters.result import Result
from utils.files import get_full_path


class Application:
    DEFAULT_DATA_SOURCE_PATH = "Datasets/Test/test.log"
    DEFAULT_PARSER = "test_log_parser_strategy"
    PARSER_STATISTICS = "Parser Statistics"
    STATISTICAL_PROFILE= "Statistical profile"
    TABLE_RESULTS = "Table Results"
    EXECUTION_CALL_TRACKER = "Execution call tracker"
    # For HDFS: "./resources/hadoop-hdfs-datanode-mesos-01.log"

    def __init__(self, application_initialization, call_tracker):
        self._application_initialization = application_initialization
        self._call_tracker = call_tracker

    def run(self):
        self._call_tracker.clear_state()

        source_dataset_path = self._get_source_dataset_path()
        parser = self._get_parser()
        formatters = self._get_formatters()
        source_rdd = self._get_source_rdd(source_dataset_path)
        parser_result = parser.parse(source_rdd)
        self._emit_parser_statistics(parser_result.parser_statistics)
        dataset_rdd_cached = parser_result.parsed_rdd.cache()
        self._emit_columnar_statistics(dataset_rdd_cached, parser_result.parsed_rdd, formatters)
        self._emit_table_statistics(dataset_rdd_cached)

        self._emit_execution_statistics()

    def _get_source_rdd(self, source_dataset_path):
        spark_context = self._application_initialization.spark_configuration.get_spark_context()
        source_rdd = self._application_initialization.reader.read(spark_context=spark_context,
                                                                  filename=source_dataset_path)
        return source_rdd

    def _get_parser(self):
        parser_class_name = self._application_initialization.interface.get_parser_or_default(self.DEFAULT_PARSER)
        parser = self._application_initialization.parser_providers.parser(parser_class_name)
        return parser

    def _get_source_dataset_path(self):
        source_dataset_path = get_full_path(Path("resources") / Path(
            self._application_initialization.interface.get_path_or_default(self.DEFAULT_DATA_SOURCE_PATH)))
        return source_dataset_path

    def _get_formatters(self):
        formatters_names = self._application_initialization.interface.get_formatters_or_default(None)
        return None if formatters_names is None else [self._application_initialization.formatter_providers.providers[formatter]()
                                                      for formatter in formatters_names.split(' ')]

    def _emit_parser_statistics(self, parser_statistics):
        result = Result(dictionary=asdict(parser_statistics))
        self._application_initialization.results_viewer.print_result(result, self.PARSER_STATISTICS)

    def _emit_table_statistics(self, dataset_rdd_cached):
        dataset_results = self._application_initialization.tuple_processor.process(dataset_rdd_cached)
        result = Result(dictionary=asdict(dataset_results))
        self._application_initialization.results_viewer.print_result(result, self.TABLE_RESULTS)

    @instrument_call
    def _emit_columnar_statistics(self, dataset_rdd_cached, parsed_rdd, formatters):
        parsed_data_frame = parsed_rdd.toDF()
        column_types = [type for field_name, type in parsed_data_frame.dtypes]
        results = self._application_initialization.row_dispatcher.dispatch(dataset_rdd_cached, column_types)
        formatted_results = self._application_initialization.results_formatter.format_results(results, formatters)
        self._application_initialization.results_viewer.print_results(formatted_results,
                                                                      self.STATISTICAL_PROFILE,
                                                                      parsed_data_frame.schema.names)

    def _emit_execution_statistics(self):
        call_trackers_dictionary = self._call_tracker.get_call_trackers_dictionary()
        result = Result(dictionary=call_trackers_dictionary)
        self._application_initialization.results_viewer.print_result(result, self.EXECUTION_CALL_TRACKER)
