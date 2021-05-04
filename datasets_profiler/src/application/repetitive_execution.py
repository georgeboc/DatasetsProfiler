from logging import getLogger


class RepetitiveExecution:
    def __init__(self, application, parameters_reader, log_initializer, arguments_parser):
        self._application = application
        self._parameters_reader = parameters_reader
        self._log_initializer = log_initializer
        self._arguments_parser = arguments_parser

    def run(self):
        self._arguments_parser.initialize()
        parsed_arguments = self._arguments_parser.parse_arguments()

        self._log_initializer.initialize(parsed_arguments.log_directory_path)
        log = getLogger(__name__)
        log.info("Datasets evaluation application successfully initialized")

        exceptions_raised = []
        log.info("Getting parameters list")
        parameters = self._parameters_reader.get_parameters(parsed_arguments.parameters_path)
        for parameters in parameters:
            log.info(f"Running application with following parameters: {parameters}")
            exception_raised = self._try_run_application(parameters, log)
            if exception_raised:
                log.error(f"Application finished with errors: {exception_raised}")
                exceptions_raised.append(exception_raised)
            else:
                log.info(f"Application finished successfully")
        if exceptions_raised:
            log.error(f"Some executions were unsuccessful")
        else:
            log.info(f"All executions were successful")

    def _try_run_application(self, parameters, log):
        try:
            self._application.run(parameters)
        except Exception as exception:
            log.exception(f"Exception raised while running application")
            return exception
