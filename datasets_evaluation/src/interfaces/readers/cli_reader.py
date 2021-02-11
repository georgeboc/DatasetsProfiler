class CLIReader:
    def __init__(self, channel):
        self._channel = channel

    def read_line(self):
        return self._channel.readline().replace('\n', '')

    def read_all(self):
        string = ""
        while line := self.read_line():
            string += line + "\n"
        return string
