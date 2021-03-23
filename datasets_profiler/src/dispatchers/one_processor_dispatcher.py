from logging import getLogger


LOG = getLogger(__name__)


class OneProcessorDispatcher:
    def __init__(self, processor):
        self._processor = processor

    def dispatch(self, column_data_frame):
        LOG.info(f"Processing column by {self._processor.__class__.__name__} processor")
        return self._processor.process(column_data_frame)
