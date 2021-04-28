from datasets_profiler.src.filesystems.filesystem import FileSystemInterface


class HDFSFileSystem(FileSystemInterface):
    ROOT_USER = "root"

    def __init__(self, hdfs_client):
        self._hdfs_client = hdfs_client

    def list(self, directory_path):
        return self._hdfs_client.listdir(directory_path)

    def makedirs(self, directory_path):
        self._hdfs_client.mkdirs(directory_path)