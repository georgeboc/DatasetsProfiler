class LocalExecutionChecker:
    def __init__(self, arguments_parser):
        self._arguments_parser = arguments_parser

    def is_local_execution(self):
        return self._arguments_parser.parse_arguments().local
