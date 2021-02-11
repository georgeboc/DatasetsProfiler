class FileReader:
    def __init__(self, filename):
        self._file = open(filename, 'r', encoding="utf-8")

    def __del__(self):
        self._file.close()

    def read_line(self):
        return self._file.readline()

    def read_all(self):
        return ''.join(self._file.readlines())
