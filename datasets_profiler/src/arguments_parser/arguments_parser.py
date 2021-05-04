class ArgumentsParser:
    REQUIRED = True
    NOT_REQUIRED = False
    DEFAULT_LOG_FOLDER = "../log"

    def __init__(self, parser):
        self._parser = parser

    def initialize(self):
        self._parser.add_argument('-p', '--parameters-path',
                                  help='parameters file path',
                                  required=self.REQUIRED)
        self._parser.add_argument('-d', '--log-directory-path',
                                  help='log directory path',
                                  required=self.NOT_REQUIRED,
                                  default=self.DEFAULT_LOG_FOLDER)
        self._parser.add_argument('-l', '--local',
                                  help='execute Spark locally if the flag is present',
                                  required=self.NOT_REQUIRED,
                                  action='store_true')

    def parse_arguments(self):
        return self._parser.parse_args()
