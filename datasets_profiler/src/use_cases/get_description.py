from dataclasses import asdict, dataclass
from logging import getLogger
from typing import Any

from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.results_formatters.result import Result

LOG = getLogger(__name__)


@dataclass
class GetDescriptionInitialization:
    spark_configuration: Any
    rdd_reader: Any
    column_dispatcher: Any
    tuple_processor: Any
    column_statistics_calculator: Any
    results_viewer: Any
    parser_providers: Any
    formatter_providers: Any
    results_formatter: Any
    checkpointer: Any


class GetDescription:
    PARSER_STATISTICS = "Parser Statistics"
    STATISTICAL_PROFILE = "Statistical profile"
    TABLE_RESULTS = "Table Results"
    EXECUTION_CALL_TRACKER = "Execution call tracker"

    def __init__(self, initialization, call_tracker):
        self._initialization = initialization
        self._call_tracker = call_tracker

    def execute(self, parameters):
        LOG.info("Getting parser")
        parser = self._get_parser(parameters)

        LOG.info("Getting formatters")
        formatters = self._get_formatters(parameters)

        LOG.info("Getting input RDD")
        input_rdd = self._get_input_rdd(parameters)

        LOG.info("Cleaning left over checkpoints")
        self._initialization.checkpointer.clean_all_checkpoints()

        LOG.info("Parsing input RDD")
        parser_result = parser.parse(input_rdd)

        LOG.info("Emitting parser statistics")
        self._emit_parser_statistics(parser_result.parser_statistics, parameters.output_path)

        LOG.info("Checkpointing parsed data frame")
        checkpointed_parsed_data_frame = self._initialization.checkpointer.checkpoint(parser_result.parsed_data_frame)

        LOG.info("Calculating columnar statistics")
        self._emit_columnar_statistics(checkpointed_parsed_data_frame, formatters, parameters.output_path)

        LOG.info("Calculating table statistics")
        self._emit_table_statistics(checkpointed_parsed_data_frame, parameters.output_path)

        if parameters.disable_execution_metadata:
            LOG.info("Skipping execution statistics emission")
        else:
            LOG.info("Emitting execution statistics")
            self._emit_execution_statistics(parameters.output_path)

        LOG.info("Cleaning all checkpoints")
        self._initialization.checkpointer.clean_all_checkpoints()

        LOG.info("Clearing call tracker state")
        self._call_tracker.clear_state()

    def _get_input_rdd(self, parameters):
        spark_session = self._initialization.spark_configuration.get_spark_session()
        source_rdd = self._initialization.rdd_reader.read(spark_session=spark_session,
                                                          filename=parameters.input_path,
                                                          limit=parameters.limit)
        return source_rdd

    def _get_parser(self, parameters):
        return self._initialization.parser_providers.parser(parameters.parser)

    def _get_formatters(self, parameters):
        return None if parameters.formatter_names is None \
            else [self._initialization.formatter_providers.get_formatter(formatter_name)
                  for formatter_name in parameters.formatter_names]

    def _emit_parser_statistics(self, parser_statistics, output_file_path):
        LOG.info("Emitting parser statistics")
        result = Result(dictionary=asdict(parser_statistics))
        self._initialization.results_viewer.print_result(result, self.PARSER_STATISTICS,
                                                         output_file_path)

    def _emit_table_statistics(self, data_frame, output_file_path):
        rdd = data_frame.rdd
        LOG.info("Calculating table statistics")
        dataset_results = self._initialization.tuple_processor.process(rdd)
        result = Result(dictionary=asdict(dataset_results))
        LOG.info("Sending table statistics to results viewer")
        self._initialization.results_viewer.print_result(result, self.TABLE_RESULTS, output_file_path)

    @instrument_call
    def _emit_columnar_statistics(self, data_frame, formatter_names, output_file_path):
        LOG.info("Dispatching columns")
        results_by_column_names = self._initialization.column_dispatcher.dispatch(data_frame)
        results = list(element[0] for element in map(lambda column_dict: list(column_dict.values()), results_by_column_names))
        LOG.info("Formatting results")
        formatted_results = self._initialization.results_formatter.format_results(results, formatter_names)
        LOG.info("Sending results to results viewer")
        self._initialization.results_viewer.print_results(formatted_results,
                                                          self.STATISTICAL_PROFILE,
                                                          data_frame.schema.names,
                                                          output_file_path)

    def _emit_execution_statistics(self, output_file_path):
        call_trackers_dictionary = self._call_tracker.get_call_trackers_dictionary()
        result = Result(dictionary=call_trackers_dictionary)
        LOG.info("Sending execution statistics to results viewer")
        self._initialization.results_viewer.print_result(result, self.EXECUTION_CALL_TRACKER,
                                                         output_file_path)
