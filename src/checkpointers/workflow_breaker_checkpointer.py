import shutil
from datetime import datetime


class StatefulWorkflowBreakerCheckpointer:
    SWAP_HDFS_PATH = "temporal/checkpoint"

    def __init__(self, spark_configuration):
        self._spark_configuration = spark_configuration
        self._created_checkpoints = []

    def checkpoint(self, data_frame):
        checkpoint_name = self._get_filename(data_frame)
        data_frame.write.save(checkpoint_name, format="parquet", mode="overwrite")
        self._created_checkpoints.append(checkpoint_name)
        return self._spark_configuration.get_spark_session().read.parquet(checkpoint_name)

    def clean_all_checkpoints(self):
        for created_checkpoint in self._created_checkpoints:
            shutil.rmtree(created_checkpoint, ignore_errors=True)
            self._created_checkpoints.remove(created_checkpoint)

    def _get_filename(self, data_frame):
        return f"{self.SWAP_HDFS_PATH}_{hash(data_frame)}_{datetime.now()}"
