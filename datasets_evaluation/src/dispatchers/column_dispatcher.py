class ColumnDispatcher:
    def __init__(self, type_dispatcher):
        self._type_dispatcher = type_dispatcher

    def dispatch(self, data_frame, column_data_types):
        columns = data_frame.columns
        return [self._type_dispatcher.dispatch(data_frame.select(columns[column_number]).rdd, data_type)
                for column_number, data_type in enumerate(column_data_types)]