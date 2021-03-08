from dataclasses import asdict

from datasets_evaluation.src.instrumentation.call_tracker import instrument_call
from datasets_evaluation.src.results_formatters.result import Result

from logging import getLogger

LOGGER = getLogger(__name__)


class Application:
    PARSER_STATISTICS = "Parser Statistics"
    STATISTICAL_PROFILE= "Statistical profile"
    TABLE_RESULTS = "Table Results"
    EXECUTION_CALL_TRACKER = "Execution call tracker"
    # For HDFS: "./resources/hadoop-hdfs-datanode-mesos-01.log"

    def __init__(self, application_initialization, call_tracker):
        self._application_initialization = application_initialization
        self._call_tracker = call_tracker

    def run(self, parameters):
        parser = self._get_parser(parameters)
        formatters = self._get_formatters(parameters)
        input_rdd = self._get_input_rdd(parameters)

        LOGGER.info("Parsing input RDD")
        parser_result = parser.parse(input_rdd)

        data_writer_interface = self._get_data_writer_interface(parameters)
        self._emit_parser_statistics(parser_result.parser_statistics, data_writer_interface)
        LOGGER.info("Checkpointing parsed data frame")
        checkpointed_parsed_data_frame = self._application_initialization.checkpointer.checkpoint(parser_result.parsed_data_frame)
        self._emit_columnar_statistics(checkpointed_parsed_data_frame, formatters, data_writer_interface)
        self._emit_table_statistics(checkpointed_parsed_data_frame, data_writer_interface)

        self._emit_execution_statistics(data_writer_interface)

        LOGGER.info("Cleaning all checkpoints")
        self._application_initialization.checkpointer.clean_all_checkpoints()
        LOGGER.info("Clearing spark cache")
        self._application_initialization.spark_configuration.get_spark_session().catalog.clearCache()
        LOGGER.info("Clearing call tracker state")
        self._call_tracker.clear_state()

    def _get_data_writer_interface(self, parameters):
        return self._application_initialization.interface_providers.data_writer_interface(parameters.output_path)

    def _get_input_rdd(self, parameters):
        spark_session = self._application_initialization.spark_configuration.get_spark_session()
        source_rdd = self._application_initialization.rdd_reader.read(spark_session=spark_session,
                                                                      filename=parameters.input_path,
                                                                      limit=parameters.limit)
        return source_rdd

    def _get_parser(self, parameters):
        return self._application_initialization.parser_providers.parser(parameters.parser)

    def _get_formatters(self, parameters):
        return None if parameters.formatters is None else [self._application_initialization.formatter_providers.providers[formatter]()
                                                           for formatter in parameters.formatters]

    def _emit_parser_statistics(self, parser_statistics, data_writer_interface):
        LOGGER.info("Emitting parser statistics")
        result = Result(dictionary=asdict(parser_statistics))
        self._application_initialization.results_viewer.print_result(result, self.PARSER_STATISTICS, data_writer_interface)

    def _emit_table_statistics(self, data_frame, data_writer_interface):
        rdd = data_frame.rdd
        LOGGER.info("Calculating table statistics")
        dataset_results = self._application_initialization.tuple_processor.process(rdd)
        result = Result(dictionary=asdict(dataset_results))
        LOGGER.info("Sending table statistics to results viewer")
        self._application_initialization.results_viewer.print_result(result, self.TABLE_RESULTS, data_writer_interface)

    @instrument_call
    def _emit_columnar_statistics(self, data_frame, formatters, data_writer_interface):
        column_types = [type for field_name, type in data_frame.dtypes]
        LOGGER.info("Calculating columnar statistics")
        LOGGER.info("Dispatching columns")
        results = self._application_initialization.column_dispatcher.dispatch(data_frame, column_types)
        LOGGER.info("Formatting results")
        formatted_results = self._application_initialization.results_formatter.format_results(results, formatters)
        LOGGER.info("Sending results to results viewer")
        self._application_initialization.results_viewer.print_results(formatted_results,
                                                                      self.STATISTICAL_PROFILE,
                                                                      data_frame.schema.names,
                                                                      data_writer_interface)

    def _emit_execution_statistics(self, data_writer_interface):
        call_trackers_dictionary = self._call_tracker.get_call_trackers_dictionary()
        result = Result(dictionary=call_trackers_dictionary)
        LOGGER.info("Sending execution statistics to results viewer")
        self._application_initialization.results_viewer.print_result(result,
                                                                     self.EXECUTION_CALL_TRACKER,
                                                                     data_writer_interface)
