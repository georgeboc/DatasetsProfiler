from abc import ABC


class FileSystemInterface(ABC):
    def list(self, directory_path):
        pass

    def makedirs(self, directory_path):
        pass
