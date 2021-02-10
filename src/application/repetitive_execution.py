import sys
import traceback

from utils.error import eprint


class RepetitiveExecution:
    BLUE_COLOR = "\033[94m"
    RED_COLOR = "\033[0;31m"
    END_COLOR = "\033[0m"
    NEW_ENTRY_FLAG = '-'*10 + '\n'

    def __init__(self, application):
        self._application = application

    def run(self):
        stdout_fd = sys.stdout
        exceptions_raised = []
        for line in sys.stdin:
            if line != self.NEW_ENTRY_FLAG:
                return
            output_file_path = input()
            eprint(f"{self.BLUE_COLOR}Running application for {output_file_path} {self.END_COLOR}")
            with open(output_file_path, 'w+') as file:
                sys.stdout = file
                try:
                    self._application.run()
                except Exception as e:
                    exceptions_raised.append(e)
                    sys.stdout = stdout_fd
                    traceback.print_exc()
                else:
                    sys.stdout = stdout_fd
        eprint(f"{self.RED_COLOR}Number of exceptions raised: {len(exceptions_raised)} {self.END_COLOR}")
        if exceptions_raised:
            eprint(f"{self.RED_COLOR}Exceptions raised during program execution: {self.END_COLOR}")
        for exception_raised in exceptions_raised:
            eprint(f"{self.RED_COLOR}  - Exception raised: {str(exception_raised)} {self.END_COLOR}")
