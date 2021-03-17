from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import Any

LOG = getLogger(__name__)


@dataclass
class GetDescribedDatasetInitialization:
    spark_configuration: Any
    rdd_reader: Any
    column_dispatcher: Any
    parser_providers: Any
    checkpointer: Any
    serializer_deserializer: Any


class GetDescribedDataset:
    PARSED_DATASET_PREFIX = "parsed_dataset"
    COUNT_VALUES_STATS_PREFIX = "count_value_stats"

    def __init__(self, initialization, call_tracker):
        self._initialization = initialization
        self._call_tracker = call_tracker

    def execute(self, parameters):
        LOG.info("Getting parser")
        parser = self._get_parser(parameters)

        LOG.info("Getting input RDD")
        input_rdd = self._get_input_rdd(parameters)

        LOG.info("Cleaning left over checkpoints")
        self._initialization.checkpointer.clean_all_checkpoints()

        LOG.info("Parsing input RDD")
        parser_result = parser.parse(input_rdd)

        LOG.info("Checkpointing parsed data frame")
        checkpointed_parsed_data_frame = self._checkpoint_parsed_data_frame(parameters, parser_result)

        LOG.info("Calculating columnar statistics")
        data_frame_results = self._initialization.column_dispatcher.dispatch(checkpointed_parsed_data_frame)

        LOG.info("Sending to serialize results by columns")
        self._serializer_results_by_columns(parameters, data_frame_results)

        LOG.info("Clearing call tracker state")
        self._call_tracker.clear_state()

    def _serializer_results_by_columns(self, parameters, data_frame_results_by_column_names):
        for data_frame_result_by_column_names in data_frame_results_by_column_names:
            column_name, data_frame_result = list(data_frame_result_by_column_names.items())[0]
            self._initialization.serializer_deserializer.serialize(data_frame_result, self._get_path(
                f"{self.COUNT_VALUES_STATS_PREFIX}_{column_name}",
                parameters.output_directory))

    def _checkpoint_parsed_data_frame(self, parameters, parser_result):
        checkpointed_parsed_data_frame = self._initialization.checkpointer.checkpoint(
            parser_result.parsed_data_frame, preferred_path=self._get_path(self.PARSED_DATASET_PREFIX,
                                                                           parameters.output_directory))
        return checkpointed_parsed_data_frame

    def _get_path(self, prefix, output_directory):
        return output_directory + '/' + prefix + '_' + Path(output_directory).stem

    def _get_parser(self, parameters):
        return self._initialization.parser_providers.parser(parameters.parser)

    def _get_input_rdd(self, parameters):
        spark_session = self._initialization.spark_configuration.get_spark_session()
        source_rdd = self._initialization.rdd_reader.read(spark_session=spark_session,
                                                          filename=parameters.input_path,
                                                          limit=parameters.limit)
        return source_rdd
