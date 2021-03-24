import os

from datasets_profiler.src.filesystems.filesystem import FileSystemInterface


class LocalFileSystem(FileSystemInterface):
    def list(self, directory_path):
        return os.listdir(directory_path)

    def makedirs(self, directory_path):
        os.makedirs(directory_path)
