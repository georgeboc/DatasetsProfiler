from datetime import datetime
from datasets_profiler.src.instrumentation.call_tracker import instrument_call

from logging import getLogger

LOG = getLogger(__name__)


class WorkflowBreakerCheckpointer:
    CHECKPOINTS_PATH = "../temporal/"
    CHECKPOINT_PREFIX = "checkpoint"

    def __init__(self, dataframe_serializer_deserializer, filesystem, call_tracker):
        self._dataframe_serializer_deserializer = dataframe_serializer_deserializer
        self._filesystem = filesystem
        self._call_tracker = call_tracker

    @instrument_call
    def checkpoint(self, data_frame):
        checkpoint_name = self._generate_filename(data_frame)
        LOG.info("Serializing to a file the data frame and breaking workflow")
        self._dataframe_serializer_deserializer.serialize(data_frame, checkpoint_name)
        LOG.info("Deserializing from a file the data frame as resuming workflow")
        return self._dataframe_serializer_deserializer.deserialize(checkpoint_name)

    def clean_all_checkpoints(self):
        self._filesystem.remove_recursively(self.CHECKPOINTS_PATH)

    def _generate_filename(self, data_frame):
        return f"{self.CHECKPOINTS_PATH}/{self.CHECKPOINT_PREFIX}_{hash(data_frame)}_{datetime.now()}"
