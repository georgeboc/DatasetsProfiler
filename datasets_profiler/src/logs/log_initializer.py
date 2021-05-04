import logging, logging.config
from datetime import datetime


class LogInitializer:
    LOG_FILENAME_KEY = "logfilename"
    FILE_CONFIG_PATH = "resources/logging.conf"
    FILENAME_PREFIX = "DatasetsProfiler"
    COLON = ':'
    HYPHEN = '-'

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
        logging.config.fileConfig(self.FILE_CONFIG_PATH, defaults={self.LOG_FILENAME_KEY: f"{log_folder}/{filename}"})
