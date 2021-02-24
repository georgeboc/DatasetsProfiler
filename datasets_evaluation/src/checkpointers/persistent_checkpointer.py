from pyspark.storagelevel import StorageLevel
from datasets_evaluation.src.instrumentation.call_tracker import instrument_call

from logging import getLogger

LOGGER = getLogger(__name__)


class PersistentCheckpointer:
    def __init__(self, spark_configuration, call_tracker):
        self._spark_configuration = spark_configuration
        self._call_tracker = call_tracker

    @instrument_call
    def checkpoint(self, data_frame):
        LOGGER.info("Clearing spark cache")
        self._spark_configuration.get_spark_session().catalog.clearCache()
        LOGGER.info("Persisting data frame in memory and disk")
        data_frame.rdd.persist(StorageLevel.MEMORY_AND_DISK)
        return data_frame

    def clean_all_checkpoints(self):
        pass
