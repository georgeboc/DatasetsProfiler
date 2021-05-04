from datasets_profiler.src.filesystems.filesystem import FileSystemInterface


class HDFSFileSystem(FileSystemInterface):
    RECURSIVE = True
    NO_OVERWRITE = False

    def __init__(self, hdfs_client):
        self._hdfs_client = hdfs_client

    def list(self, directory_path):
        return self._hdfs_client.listdir(directory_path)

    def makedirs(self, directory_path):
        self._hdfs_client.mkdirs(directory_path)

    def remove_recursively(self, directory_path):
        self._hdfs_client.delete(directory_path, self.RECURSIVE)

    def read_file(self, file_path):
        with self._hdfs_client.open(file_path) as file:
            return '\n'.join(file.readlines())

    def write_file(self, contents, file_path):
        self._hdfs_client.create(file_path, contents, overwrite=self.NO_OVERWRITE)
