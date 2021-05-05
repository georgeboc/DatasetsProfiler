import os
import shutil

from datasets_profiler.src.filesystems.filesystem import FileSystemInterface


class LocalFileSystem(FileSystemInterface):
    IGNORE_ERRORS = True

    def list(self, directory_path):
        return os.listdir(directory_path)

    def makedirs(self, directory_path):
        os.makedirs(directory_path)

    def remove_recursively(self, directory_path):
        shutil.rmtree(directory_path, ignore_errors=self.IGNORE_ERRORS)

    def read(self, file_path):
        with open(file_path, 'rb') as file:
            return file.read()

    def write(self, contents, file_path):
        with open(file_path, 'ab+') as file:
            file.write(contents)
