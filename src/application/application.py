from utils.files import get_full_path


class Application:
    # For HDFS: "./resources/hadoop-hdfs-datanode-mesos-01.log"
    SOURCE_DATASET_PATH = get_full_path("resources/test.log") # hadoop-hdfs-datanode-mesos-01

    def __init__(self, reader, data_frame_transformer, row_dispatcher, column_statistics_calculator):
        self._reader = reader
        self._data_frame_transformer = data_frame_transformer
        self._row_dispatcher = row_dispatcher
        self._column_statistics_calculator = column_statistics_calculator

    def run(self):
        data_frame = self._reader.read(filename=self.SOURCE_DATASET_PATH,
                                       separator=": ",
                                       header=False)
        transformed_data_frame = self._data_frame_transformer.transform(data_frame)
        dataset_rdd_cached = transformed_data_frame.rdd.cache()
        print(self._row_dispatcher.dispatch(dataset_rdd_cached, len(transformed_data_frame.columns)))

        key_value_rdd = dataset_rdd_cached.map(lambda row: (1, row))
        data_source_entropy = self._column_statistics_calculator.calculate_entropy(key_value_rdd)
        print("Data source entropy:", data_source_entropy)
