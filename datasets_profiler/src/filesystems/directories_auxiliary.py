from pathlib import Path


class DirectoriesAuxiliary:
    def __init__(self, filesystem):
        self._filesystem = filesystem

    def try_create_directory(self, file_path):
        try:
            self._filesystem.makedirs(Path(file_path).parent)
        except OSError:
            pass