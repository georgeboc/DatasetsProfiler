from utils.files import get_full_path


class Application:
    # For HDFS: "./resources/hadoop-hdfs-datanode-mesos-01.log"
    SOURCE_DATASET_PATH = get_full_path("resources/test.log") # hadoop-hdfs-datanode-mesos-01

    def __init__(self, reader, data_frame_transformer, row_dispatcher):
        self._reader = reader
        self._data_frame_transformer = data_frame_transformer
        self._row_dispatcher = row_dispatcher

    def run(self):
        data_frame = self._reader.read(filename=self.SOURCE_DATASET_PATH,
                                       separator=": ",
                                       header=False)
        transformed_data_frame = self._data_frame_transformer.transform(data_frame)
        print(self._row_dispatcher.dispatch(transformed_data_frame.rdd.cache(), len(transformed_data_frame.columns)))
