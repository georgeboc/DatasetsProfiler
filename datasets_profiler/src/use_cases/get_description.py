from dataclasses import asdict
from logging import getLogger

from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.results_formatters.result import Result

LOG = getLogger(__name__)


class GetDescription:
    PARSER_STATISTICS = "Parser Statistics"
    STATISTICAL_PROFILE = "Statistical profile"
    TABLE_RESULTS = "Table Results"
    EXECUTION_CALL_TRACKER = "Execution call tracker"

    def __init__(self, application_initialization, call_tracker):
        self._application_initialization = application_initialization
        self._call_tracker = call_tracker

    def execute(self, parameters):
        parser = self._get_parser(parameters)
        formatters = self._get_formatters(parameters)
        input_rdd = self._get_input_rdd(parameters)

        LOG.info("Parsing input RDD")
        parser_result = parser.parse(input_rdd)

        self._emit_parser_statistics(parser_result.parser_statistics, parameters.output_path)
        LOG.info("Checkpointing parsed data frame")
        checkpointed_parsed_data_frame = self._application_initialization.checkpointer.checkpoint(
            parser_result.parsed_data_frame)
        self._emit_columnar_statistics(checkpointed_parsed_data_frame, formatters, parameters.output_path)
        self._emit_table_statistics(checkpointed_parsed_data_frame, parameters.output_path)

        self._emit_execution_statistics(parameters.output_path)

        LOG.info("Cleaning all checkpoints")
        self._application_initialization.checkpointer.clean_all_checkpoints()
        LOG.info("Clearing spark cache")
        self._application_initialization.spark_configuration.get_spark_session().catalog.clearCache()
        LOG.info("Clearing call tracker state")
        self._call_tracker.clear_state()

    def _get_input_rdd(self, parameters):
        spark_session = self._application_initialization.spark_configuration.get_spark_session()
        source_rdd = self._application_initialization.rdd_reader.read(spark_session=spark_session,
                                                                      filename=parameters.input_path,
                                                                      limit=parameters.limit)
        return source_rdd

    def _get_parser(self, parameters):
        return self._application_initialization.parser_providers.parser(parameters.parser)

    def _get_formatters(self, parameters):
        return None if parameters.formatters is None \
            else [self._application_initialization.formatter_providers.providers[formatter]()
                  for formatter in parameters.formatters]

    def _emit_parser_statistics(self, parser_statistics, output_file_path):
        LOG.info("Emitting parser statistics")
        result = Result(dictionary=asdict(parser_statistics))
        self._application_initialization.results_viewer.print_result(result, self.PARSER_STATISTICS,
                                                                     output_file_path)

    def _emit_table_statistics(self, data_frame, output_file_path):
        rdd = data_frame.rdd
        LOG.info("Calculating table statistics")
        dataset_results = self._application_initialization.tuple_processor.process(rdd)
        result = Result(dictionary=asdict(dataset_results))
        LOG.info("Sending table statistics to results viewer")
        self._application_initialization.results_viewer.print_result(result, self.TABLE_RESULTS, output_file_path)

    @instrument_call
    def _emit_columnar_statistics(self, data_frame, formatters, output_file_path):
        LOG.info("Calculating columnar statistics")
        LOG.info("Dispatching columns")
        results = self._application_initialization.column_dispatcher.dispatch(data_frame)
        LOG.info("Formatting results")
        formatted_results = self._application_initialization.results_formatter.format_results(results, formatters)
        LOG.info("Sending results to results viewer")
        self._application_initialization.results_viewer.print_results(formatted_results,
                                                                      self.STATISTICAL_PROFILE,
                                                                      data_frame.schema.names,
                                                                      output_file_path)

    def _emit_execution_statistics(self, output_file_path):
        call_trackers_dictionary = self._call_tracker.get_call_trackers_dictionary()
        result = Result(dictionary=call_trackers_dictionary)
        LOG.info("Sending execution statistics to results viewer")
        self._application_initialization.results_viewer.print_result(result, self.EXECUTION_CALL_TRACKER,
                                                                     output_file_path)
