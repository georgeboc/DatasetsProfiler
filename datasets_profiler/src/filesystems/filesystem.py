from abc import ABC


class FileSystemInterface(ABC):
    def list(self, directory_path):
        pass

    def makedirs(self, directory_path):
        pass

    def remove_recursively(self, directory_path):
        pass

    def read_file(self, file_path):
        pass

    def write_file(self, contents, file_path):
        pass
