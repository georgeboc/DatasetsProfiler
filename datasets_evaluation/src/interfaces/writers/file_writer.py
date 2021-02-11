class FileWriter:
    def __init__(self, filename):
        self._file = open(filename, 'w+', encoding="utf-8")

    def __del__(self):
        self._file.close()

    def write_all(self, string):
        self._file.write(string)

    def write_line(self, line):
        self.write_all(line + "\n")
