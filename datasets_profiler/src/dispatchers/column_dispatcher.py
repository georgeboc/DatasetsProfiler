class ColumnDispatcher:
    def __init__(self, specific_dispatcher):
        self._specific_dispatcher = specific_dispatcher

    def dispatch(self, data_frame):
        columns = data_frame.columns
        return [self._specific_dispatcher.dispatch(data_frame.select(column)) for column in columns]