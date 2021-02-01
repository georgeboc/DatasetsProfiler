class RowDispatcher:
    def __init__(self, field_dispatcher):
        self._field_dispatcher = field_dispatcher

    def dispatch(self, rdd_cached, count_columns):
        return [self._field_dispatcher.dispatch(rdd_cached.map(lambda row: (row[column_number],)))
                for column_number in range(count_columns)]