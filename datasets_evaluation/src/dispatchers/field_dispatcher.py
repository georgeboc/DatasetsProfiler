from logging import getLogger


LOGGER = getLogger(__name__)


class FieldDispatcher:
    WITHOUT_REPLACEMENT = "false"

    def __init__(self, type_processors):
        self._type_processors = type_processors

    def dispatch(self, column_rdd, column_data_type):
        LOGGER.info(f"Processing column RDD with type {column_data_type} by {self._type_processors[column_data_type].__class__.__name__} processor")
        result = self._type_processors[column_data_type].process(column_rdd)
        LOGGER.info(f"Processing of column RDD with type {column_data_type} by {self._type_processors[column_data_type].__class__.__name__} processor has finished")
        return result
