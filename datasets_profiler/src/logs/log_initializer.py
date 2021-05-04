import logging, logging.config
from datetime import datetime
from pkgutil import get_data
from io import StringIO


class LogInitializer:
    LOG_FILENAME_KEY = "logfilename"
    CONFIG_FILENAME = "logging.conf"
    RESOURCES_MODULE = "datasets_profiler.resources"
    FILENAME_PREFIX = "DatasetsProfiler"
    COLON = ':'
    HYPHEN = '-'
    UTF8 = "utf-8"

    def __init__(self, filesystem):
        self._filesystem = filesystem

    def initialize(self, log_folder):
        try:
            self._filesystem.makedirs(log_folder)
        except FileExistsError:
            pass
        timestamp = datetime.now().isoformat()
        timestamp_string = timestamp.replace(self.COLON, self.HYPHEN)
        filename = f"{self.FILENAME_PREFIX}_{timestamp_string}.log"
        logging_configuration = get_data(self.RESOURCES_MODULE, self.CONFIG_FILENAME).decode(self.UTF8)
        with StringIO(logging_configuration) as file:
            logging.config.fileConfig(file, defaults={self.LOG_FILENAME_KEY: f"{log_folder}/{filename}"})
