class RDDTextReader:
    def __init__(self, spark_configuration):
        self._spark_configuration = spark_configuration

    def read(self, filename, limit=None):
        spark_session = self._spark_configuration.get_spark_session()
        file_dataframe = spark_session.read.text(filename)
        non_blank_rows_dataframe = file_dataframe.filter(file_dataframe.value != '')
        return self._limit_if_a_limit_exists(non_blank_rows_dataframe, limit).rdd

    def _limit_if_a_limit_exists(self, dataframe, limit):
        return dataframe.limit(int(limit)) if limit else dataframe
