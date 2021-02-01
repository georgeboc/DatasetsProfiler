class RowDispatcher:
    def __init__(self, field_dispatcher):
        self._field_dispatcher = field_dispatcher

    def dispatch(self, rdd_cached, column_data_types):
        return [self._field_dispatcher.dispatch(rdd_cached.map(lambda row: (row[column_number],)), data_type)
                for column_number, data_type in enumerate(column_data_types)]