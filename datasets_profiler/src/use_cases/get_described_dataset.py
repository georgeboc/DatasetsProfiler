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
        checkpoint = self._checkpoint(parameters, parser_result)

        LOG.info("Calculating columnar statistics")
        columnar_statistics = self._initialization.column_dispatcher.dispatch(checkpoint)

        LOG.info("Formatting results")
        formatted_results = self._initialization.formatter.format(columnar_statistics)

        LOG.info("Creating a view of the results")
        self._initialization.viewer.view(formatted_results, self.COUNT_VALUES_STATS, parameters.output_directory)

    def _checkpoint(self, parameters, parser_result):
        return self._initialization.checkpointer.checkpoint(parser_result.parsed_data_frame,
                                                            output_path= self._get_path(parameters.output_directory,
                                                                                        self.PARSED_DATASET_PREFIX))

    def _get_path(self, output_directory, prefix):
        return output_directory + '/' + prefix
