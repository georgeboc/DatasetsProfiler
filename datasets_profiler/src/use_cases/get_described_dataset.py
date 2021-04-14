from dataclasses import dataclass
from logging import getLogger
from typing import Any

LOG = getLogger(__name__)


@dataclass
class GetDescribedDatasetInitialization:
    spark_configuration: Any
    rdd_reader: Any
    column_dispatcher: Any
    parser_providers: Any
    checkpointer: Any
    formatter: Any
    viewer: Any
    data_frame_stringifier: Any
    data_frame_serializer: Any


class GetDescribedDataset:
    PARSED_DATASET_PREFIX = "parsed_dataset"
    COUNT_VALUES_STATS = "count_value_stats"

    def __init__(self, initialization):
        self._initialization = initialization

    def execute(self, parameters):
        LOG.info("Getting parser")
        parser = self._initialization.parser_providers.get_parser_by_name(parameters.parser)

        LOG.info("Getting input RDD")
        input_rdd = self._initialization.rdd_reader.read(filename=parameters.input_path, limit=parameters.limit)

        LOG.info("Cleaning left over checkpoints")
        self._initialization.checkpointer.clean_all_checkpoints()

        LOG.info("Parsing input RDD")
        parser_result = parser.parse(input_rdd)

        LOG.info("Checkpointing parsed data frame")
        checkpoint = self._initialization.checkpointer.checkpoint(parser_result.parsed_data_frame)

        LOG.info("Stringifying parsed data frame")
        stringyfied_data_frame = self._initialization.data_frame_stringifier.stringify(checkpoint)

        LOG.info("Serializing stringified data frame")
        self._initialization.data_frame_serializer.serialize(stringyfied_data_frame,
                                                             self._get_path(parameters.output_directory,
                                                                            self.PARSED_DATASET_PREFIX))

        LOG.info("Calculating columnar statistics")
        columnar_statistics = self._initialization.column_dispatcher.dispatch(checkpoint)

        LOG.info("Formatting results")
        formatted_results = self._initialization.formatter.format(columnar_statistics)

        LOG.info("Sending results to viewer")
        self._initialization.viewer.view(formatted_results, self.COUNT_VALUES_STATS, parameters.output_directory)

    def _get_path(self, output_directory, prefix):
        return output_directory + '/' + prefix
