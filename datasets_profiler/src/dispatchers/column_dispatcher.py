from collections import OrderedDict


class ColumnDispatcher:
    def __init__(self, specific_dispatcher):
        self._specific_dispatcher = specific_dispatcher

    def dispatch(self, data_frame):
        columns = data_frame.columns
        result_dict = OrderedDict()
        for column in columns:
            result_dict[column] = self._specific_dispatcher.dispatch(data_frame.select(column))
        return result_dict
