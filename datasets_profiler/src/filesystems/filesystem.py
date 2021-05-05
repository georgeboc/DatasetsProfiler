from abc import ABC


class FileSystemInterface(ABC):
    def list(self, directory_path):
        pass

    def makedirs(self, directory_path):
        pass

    def remove_recursively(self, directory_path):
        pass

    def read(self, file_path):
        pass

    def write(self, contents, file_path):
        pass
