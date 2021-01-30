class Application:
    def __init__(self, reader, data_frame_transformer, row_dispatcher):
        self._reader = reader
        self._data_frame_transformer = data_frame_transformer
        self._row_dispatcher = row_dispatcher

    def run(self):
        data_frame = self._reader.read(filename="./resources/hadoop-hdfs-datanode-mesos-01.log",
                                       separator=": ",
                                       header=False)
        transformed_data_frame = self._data_frame_transformer.transform(data_frame)
        print(self._row_dispatcher.dispatch(transformed_data_frame.rdd.cache))
