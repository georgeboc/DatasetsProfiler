from pyspark.sql.functions import col, unix_timestamp


class DataFrameStringifier:
    STRING = "string"
    TIMESTAMP_TYPE = "timestamp"

    def stringify(self, data_frame):
        return data_frame.select([self._cast_to_string(data_frame, column) for column in data_frame.columns])

    def _cast_to_string(self, data_frame, column):
        if self._get_column_type(data_frame, column) == self.TIMESTAMP_TYPE:
            return (unix_timestamp(col(column))*1000).cast(self.STRING).alias(column)
        return col(column).cast(self.STRING)

    def _get_column_type(self, data_frame, searched_column_name):
        column_types = [column_type for column_name, column_type in data_frame.dtypes if column_name == searched_column_name]
        return column_types[0]
