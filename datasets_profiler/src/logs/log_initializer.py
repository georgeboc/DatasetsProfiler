import logging
from datetime import datetime


class LogInitializer:
    FORMAT = '%(asctime)-15s [%(levelname)s] line %(lineno)d in %(filename)s %(module)s: %(message)s'
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
        logging.basicConfig(format=self.FORMAT, filename=f"{log_folder}/{filename}", level=logging.INFO)
