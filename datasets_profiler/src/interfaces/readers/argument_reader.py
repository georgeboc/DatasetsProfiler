import sys


class ArgumentReader:

    def __init__(self):
        self._invocations = 0

    def read_line(self):
        self._invocations += 1
        if self._invocations < len(sys.argv):
            return sys.argv[self._invocations]
        return ""

    def read_all(self):
        string = ""
        while line := self.read_line():
            string += line + "\n"
        return string
