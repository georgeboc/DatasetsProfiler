class RDDTextReader:
    def read(self, spark_session, filename, limit=None):
        file_dataframe = spark_session.read.text(filename)
        non_blank_rows_dataframe = file_dataframe.filter(file_dataframe.value != '')
        return self._limit_if_a_limit_exists(non_blank_rows_dataframe, limit).rdd

    def _limit_if_a_limit_exists(self, dataframe, limit):
        return dataframe.limit(int(limit)) if limit else dataframe
