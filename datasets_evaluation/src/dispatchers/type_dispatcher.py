from logging import getLogger


LOG = getLogger(__name__)


class TypeDispatcher:
    WITHOUT_REPLACEMENT = "false"

    def __init__(self, type_processors, spark_configuration):
        self._type_processors = type_processors
        self._spark_configuration = spark_configuration

    def dispatch(self, column_rdd, column_data_type):
        processor = self._type_processors[column_data_type]
        LOG.info(f"Processing column RDD with {column_data_type} type by {processor.__class__.__name__} processor")
        result = processor.process(column_rdd)
        LOG.info(f"Processing of column RDD with {column_data_type} type by {processor.__class__.__name__} processor has finished")
        LOG.info("Clearing spark cache")
        self._spark_configuration.get_spark_session().catalog.clearCache()
        return result