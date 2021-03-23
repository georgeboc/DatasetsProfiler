from dataclasses import asdict, dataclass
from logging import getLogger
from typing import Any

from datasets_profiler.src.formatters.formatter import Result
from datasets_profiler.src.instrumentation.call_tracker import instrument_call

LOG = getLogger(__name__)


@dataclass
class GetDescriptionInitialization:
    spark_configuration: Any
    rdd_reader: Any
    column_dispatcher: Any
    tuple_processor: Any
    column_statistics_calculator: Any
    viewer: Any
    parser_providers: Any
    specific_formatters_providers: Any
    formatter: Any
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
        parser = self._initialization.parser_providers.get_parser_by_name(parameters.parser)

        LOG.info("Getting specific_formatters")
        specific_formatters = self._get_specific_formatters(parameters)

        LOG.info("Getting input RDD")
        input_rdd = self._initialization.rdd_reader.read(filename=parameters.input_path, limit=parameters.limit)

        LOG.info("Cleaning left over checkpoints")
        self._initialization.checkpointer.clean_all_checkpoints()

        LOG.info("Parsing input RDD")
        parser_result = parser.parse(input_rdd)

        LOG.info("Emitting parser statistics")
        self._emit_parser_statistics(parser_result.parser_statistics, parameters.output_path)

        LOG.info("Checkpointing parsed data frame")
        checkpoint = self._initialization.checkpointer.checkpoint(parser_result.parsed_data_frame)

        LOG.info("Calculating columnar statistics")
        self._emit_columnar_statistics(checkpoint, specific_formatters, parameters.output_path)

        LOG.info("Calculating table statistics")
        self._emit_table_statistics(checkpoint, parameters.output_path)

        if parameters.disable_execution_metadata:
            LOG.info("Skipping execution statistics emission")
        else:
            LOG.info("Emitting execution statistics")
            self._emit_execution_statistics(parameters.output_path)

        LOG.info("Cleaning all checkpoints")
        self._initialization.checkpointer.clean_all_checkpoints()

        LOG.info("Clearing call tracker state")
        self._call_tracker.clear_state()

    def _get_specific_formatters(self, parameters):
        return [self._initialization.specific_formatters_providers.get_specific_formatter_by_name(specific_formatter)
                for specific_formatter in parameters.specific_formatters] if parameters.specific_formatters else None

    def _emit_parser_statistics(self, parser_statistics, output_file_path):
        LOG.info("Emitting parser statistics")
        result = Result(dictionary=asdict(parser_statistics), column_name="Value")
        self._initialization.viewer.view([result], self.PARSER_STATISTICS, output_file_path)

    def _emit_table_statistics(self, data_frame, output_file_path):
        LOG.info("Calculating table statistics")
        dataset_results = self._initialization.tuple_processor.process(data_frame.rdd)
        result = Result(dictionary=asdict(dataset_results), column_name="Value")
        LOG.info("Sending table statistics to results viewer")
        self._initialization.viewer.view([result], self.TABLE_RESULTS, output_file_path)

    @instrument_call
    def _emit_columnar_statistics(self, data_frame, specific_formatters, output_file_path):
        LOG.info("Dispatching columns")
        columnar_statistics = self._initialization.column_dispatcher.dispatch(data_frame)
        LOG.info("Formatting results")
        formatted_results = self._initialization.formatter.format(columnar_statistics, specific_formatters)
        LOG.info("Sending results to results viewer")
        self._initialization.viewer.view(formatted_results, self.STATISTICAL_PROFILE, output_file_path)

    def _emit_execution_statistics(self, output_file_path):
        call_trackers_dictionary = self._call_tracker.get_call_trackers_dictionary()
        result = Result(dictionary=call_trackers_dictionary, column_name="Value")
        LOG.info("Sending execution statistics to results viewer")
        self._initialization.viewer.view(result, self.EXECUTION_CALL_TRACKER, output_file_path)
