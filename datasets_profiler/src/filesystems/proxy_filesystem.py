from datasets_profiler.src.filesystems.filesystem import FileSystemInterface


class ProxyFilesystem(FileSystemInterface):
    def __init__(self, local_filesystem, distributed_filesystem, local_execution_checker):
        self._local_filesystem = local_filesystem
        self._distributed_filesystem = distributed_filesystem
        self._local_execution_checker = local_execution_checker

    def list(self, directory_path):
        return self._get_filesystem().list(directory_path)

    def makedirs(self, directory_path):
        self._get_filesystem().makedirs(directory_path)

    def remove_recursively(self, directory_path):
        self._get_filesystem().remove_recursively(directory_path)

    def read_file(self, file_path):
        return self._get_filesystem().read_file(file_path)

    def write_file(self, contents, file_path):
        self._get_filesystem().write_file(contents, file_path)

    def _get_filesystem(self):
        if self._local_execution_checker.is_local_execution():
            return self._local_filesystem
        return self._distributed_filesystem
