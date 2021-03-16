import shutil
from datetime import datetime
from datasets_profiler.src.instrumentation.call_tracker import instrument_call

from logging import getLogger

LOG = getLogger(__name__)


class StatefulWorkflowBreakerCheckpointer:
    SWAP_HDFS_PATH = "../temporal/checkpoint"

    def __init__(self, dataframe_serializer_deserializer, call_tracker):
        self._dataframe_serializer_deserializer = dataframe_serializer_deserializer
        self._call_tracker = call_tracker
        self._created_checkpoints = []

    @instrument_call
    def checkpoint(self, data_frame):
        checkpoint_name = self._get_filename(data_frame)
        LOG.info("Serializing to a file the data frame and breaking workflow")
        self._dataframe_serializer_deserializer.serialize(data_frame, checkpoint_name)
        self._created_checkpoints.append(checkpoint_name)
        LOG.info("Deserializing from a file the data frame as resuming workflow")
        return self._dataframe_serializer_deserializer.deserialize(checkpoint_name)

    def clean_all_checkpoints(self):
        for created_checkpoint in self._created_checkpoints:
            shutil.rmtree(created_checkpoint, ignore_errors=True)
            self._created_checkpoints.remove(created_checkpoint)

    def _get_filename(self, data_frame):
        return f"{self.SWAP_HDFS_PATH}_{hash(data_frame)}_{datetime.now()}"
