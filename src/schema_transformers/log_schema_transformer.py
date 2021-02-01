from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, LongType


class LogDataFrameTransformer:
    def transform(self, source_data_frame):
        source_rdd = source_data_frame.rdd
        header_split_rdd = source_rdd.map(self._split_header)
        filter_out_non_parseable_rows_rdd = header_split_rdd.filter(bool)
        return self._rename_data_frame_columns(filter_out_non_parseable_rows_rdd.toDF(), {
            "_1": (TimestampType, "DateTime"),
            "_2": (StringType, "LogLevel"),
            "_3": (StringType, "ClassName"),
            "_4": (LongType, "Message")
        })

    def _rename_data_frame_columns(self, data_frame, columns: dict):
        data_frame_renamed = data_frame
        for key, datatype_name in columns.items():
            datatype, name = datatype_name
            data_frame_renamed = data_frame_renamed.withColumn(name, data_frame_renamed[key].cast(datatype()))
        field_names = [datatype_name[1] for datatype_name in columns.values()]
        return data_frame_renamed.select(*field_names)

    def _split_header(self, row):
        header = row[0]
        #messages = [message for message in row[1:] if message]
        message = row[1] #": ".join(messages)
        try:
            date, time, log_level, class_name = header.split(' ')
            date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S,%f')
            return date_time, log_level, class_name, message
        except ValueError:
            return
