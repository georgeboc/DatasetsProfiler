from datetime import datetime


class LogDataFrameTransformer:
    def transform(self, source_data_frame):
        renamed_schemas_source_data_frame = source_data_frame.withColumnRenamed("_c0", "Header") \
            .withColumnRenamed("_c1", "Message1") \
            .withColumnRenamed("_c2", "Message2")
        renamed_source_rdd = renamed_schemas_source_data_frame.rdd

        header_message_rdd = renamed_source_rdd.map(self._merge_messages)
        header_message_rdd_renamed = self._rename_columns(header_message_rdd, {
            "_1": "Header",
            "_2": "Message"
        })

        def split_header(row):
            header, message = row
            try:
                date, time, log_level, class_name = header.split(' ')
                date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S,%f')
                return date_time, log_level, class_name, message
            except ValueError:
                return

        header_split_rdd = header_message_rdd_renamed.map(split_header).filter(lambda row: bool(row))
        return self._rename_columns(header_split_rdd, {
            "_1": "DateTime",
            "_2": "LogLevel",
            "_3": "ClassName",
            "_4": "Message"
        })

    def _merge_messages(self, row):
        header, message1, message2 = row
        if not message2:
            return header, message1
        return header, message1 + message2

    def _rename_columns(self, rdd, columns: dict):
        data_frame = rdd.toDF()
        data_frame_renamed = data_frame
        for key, value in columns.items():
            data_frame_renamed = data_frame_renamed.withColumnRenamed(key, value)
        return data_frame_renamed
