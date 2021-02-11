class RowDispatcher:
    def __init__(self, field_dispatcher):
        self._field_dispatcher = field_dispatcher

    def dispatch(self, data_frame, column_data_types):
        columns = data_frame.columns
        return [self._field_dispatcher.dispatch(data_frame.select(columns[column_number]).rdd, data_type)
                for column_number, data_type in enumerate(column_data_types)]