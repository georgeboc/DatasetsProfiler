import sys

from utils.error import eprint


class RepetitiveExecution:
    BLUE_COLOR = "\033[94m"
    END_COLOR = "\033[0m"
    NEW_ENTRY_FLAG = '-'*10 + '\n'

    def __init__(self, application):
        self._application = application

    def run(self):
        stdout_fd = sys.stdout
        for line in sys.stdin:
            if line != self.NEW_ENTRY_FLAG:
                return
            output_file_path = input()
            eprint(f"{self.BLUE_COLOR}Running application for {output_file_path}{self.END_COLOR}")
            with open(output_file_path, 'w+') as file:
                sys.stdout = file
                self._application.run()
                sys.stdout = stdout_fd
