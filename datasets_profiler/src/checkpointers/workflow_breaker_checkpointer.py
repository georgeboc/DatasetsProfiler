from datetime import datetime
from datasets_profiler.src.instrumentation.call_tracker import instrument_call

from logging import getLogger

LOG = getLogger(__name__)


class WorkflowBreakerCheckpointer:
    LOCAL_CHECKPOINTS_PATH = "../temporal/"
    CLUSTER_CHECKPOINTS_PATH = "/user/bochileanu/temporal/"
    CHECKPOINT_PREFIX = "checkpoint"

    def __init__(self, dataframe_serializer_deserializer, filesystem, call_tracker, local_execution_checker):
        self._dataframe_serializer_deserializer = dataframe_serializer_deserializer
        self._filesystem = filesystem
        self._call_tracker = call_tracker
        self._local_execution_checker = local_execution_checker

    @instrument_call
    def checkpoint(self, data_frame):
        checkpoint_name = self._generate_filename(data_frame)
        LOG.info("Serializing to a file the data frame and breaking workflow")
        self._dataframe_serializer_deserializer.serialize(data_frame, checkpoint_name)
        LOG.info("Deserializing from a file the data frame as resuming workflow")
        return self._dataframe_serializer_deserializer.deserialize(checkpoint_name)

    def clean_all_checkpoints(self):
        self._filesystem.remove_recursively(self._get_checkpoints_path())

    def _generate_filename(self, data_frame):
        return f"{self._get_checkpoints_path()}/{self.CHECKPOINT_PREFIX}_{hash(data_frame)}_{datetime.now()}"

    def _get_checkpoints_path(self):
        if self._local_execution_checker.is_local_execution():
            return self.LOCAL_CHECKPOINTS_PATH
        return self.CLUSTER_CHECKPOINTS_PATH
