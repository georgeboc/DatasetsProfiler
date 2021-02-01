from pathlib import Path

from pyspark.sql.dataframe import DataFrame

from utils.files import get_full_path


class Application:
    DEFAULT_DATA_SOURCE_PATH = "Datasets/Test/test.log"
    DEFAULT_DATA_FRAME_TRANSFORMER = "LogDataFrameTransformerWithInteger"
    # For HDFS: "./resources/hadoop-hdfs-datanode-mesos-01.log"

    def __init__(self, reader, row_dispatcher, column_statistics_calculator, results_viewer, command_line_interface,
                 data_frame_transformer_dispatcher):
        self._reader = reader
        self._row_dispatcher = row_dispatcher
        self._column_statistics_calculator = column_statistics_calculator
        self._results_viewer = results_viewer
        self._command_line_interface = command_line_interface
        self._data_frame_transformer_dispatcher = data_frame_transformer_dispatcher

    def run(self):
        source_dataset_path = get_full_path(Path("resources") / Path(self._command_line_interface.get_path_or_default(self.DEFAULT_DATA_SOURCE_PATH)))
        data_frame_transformer_class_name = self._command_line_interface.get_schema_transformer_or_default(self.DEFAULT_DATA_FRAME_TRANSFORMER)
        data_frame_transformer = self._data_frame_transformer_dispatcher.dispatch(data_frame_transformer_class_name)
        data_frame = self._reader.read(filename=source_dataset_path,
                                       separator=": ",
                                       header=False)
        transformed_data_frame: DataFrame = data_frame_transformer.transform(data_frame)
        dataset_rdd_cached = transformed_data_frame.rdd.cache()
        column_types = [type for field_name, type in transformed_data_frame.dtypes]
        results = self._row_dispatcher.dispatch(dataset_rdd_cached, len(transformed_data_frame.columns), column_types)
        self._results_viewer.print_results(results, transformed_data_frame.schema.names)

        key_value_rdd = dataset_rdd_cached.map(lambda row: (1, row))
        data_source_entropy = self._column_statistics_calculator.calculate_entropy(key_value_rdd)
        print("Data source entropy:", data_source_entropy)
