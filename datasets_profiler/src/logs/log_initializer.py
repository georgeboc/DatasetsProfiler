import logging, logging.config
from datetime import datetime
from pkgutil import get_data
from io import StringIO

from datasets_profiler.src.utils.timestamp_utils import TimestampUtils


class LogInitializer:
    LOG_FILENAME_KEY = "logfilename"
    CONFIG_FILENAME = "logging.conf"
    RESOURCES_MODULE = "datasets_profiler.resources"
    FILENAME_PREFIX = "DatasetsProfiler"
    UTF8 = "utf-8"

    def __init__(self, filesystem):
        self._filesystem = filesystem

    def initialize(self, log_folder):
        try:
            self._filesystem.makedirs(log_folder)
        except FileExistsError:
            pass
        filename = f"{self.FILENAME_PREFIX}_{TimestampUtils.get_file_name_from_now()}.log"
        logging_configuration = get_data(self.RESOURCES_MODULE, self.CONFIG_FILENAME).decode(self.UTF8)
        with StringIO(logging_configuration) as file:
            logging.config.fileConfig(file, defaults={self.LOG_FILENAME_KEY: f"{log_folder}/{filename}"})
