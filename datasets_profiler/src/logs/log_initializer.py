import logging
from datetime import datetime
import os


class LogInitializer:
    FORMAT = '%(asctime)-15s [%(levelname)s] line %(lineno)d in %(filename)s %(module)s: %(message)s'
    FILENAME_PREFIX = "DatasetsProfiler"

    def initialize(self, log_folder):
        try:
            os.makedirs(log_folder)
        except FileExistsError:
            pass
        filename = f"{self.FILENAME_PREFIX}_{datetime.now().isoformat()}.log"
        logging.basicConfig(format=self.FORMAT, filename=f"{log_folder}/{filename}", level=logging.INFO)
