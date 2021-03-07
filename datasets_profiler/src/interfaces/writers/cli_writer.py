class CLIWriter:
    def __init__(self, channel):
        self._channel = channel

    def write_all(self, string):
        self._channel.write(string)

    def write_line(self, line):
        self.write_all(line + "\n")
