from logging import getLogger


class RepetitiveExecution:
    GREEN_COLOR = "\033[92m"
    BLUE_COLOR = "\033[94m"
    CYAN_COLOR = "\033[36m"
    RED_COLOR = "\033[0;31m"
    END_COLOR = "\033[0m"

    def __init__(self, application, parameters_reader, interface_providers, log_initializer, arguments_parser):
        self._application = application
        self._parameters_reader = parameters_reader
        self._interface_providers = interface_providers
        self._log_initializer = log_initializer
        self._arguments_parser = arguments_parser

    def run(self):
        control_writer_interface = self._interface_providers.control_writer_interface()
        self._arguments_parser.initialize()
        parsed_arguments = self._arguments_parser.parse_arguments()

        self._log_initializer.initialize(parsed_arguments.log_directory_path)
        log = getLogger(__name__)
        log.info("Datasets evaluation application successfully initialized")

        exceptions_raised = []
        log.info("Getting parameters list")
        parameters = self._parameters_reader.get_parameters(parsed_arguments.parameters_path)
        for parameters in parameters:
            control_writer_interface.write_line(
                self._paint(f"Running application with input path: {parameters.input_path}", self.BLUE_COLOR))
            log.info(f"Running application with following parameters: {parameters}")
            exception_raised = self._try_run_application(parameters, log)
            if exception_raised:
                control_writer_interface.write_line(
                    self._paint(f"Application finished with errors: {exception_raised}", self.RED_COLOR))
                log.error(f"Application finished with errors: {exception_raised}")
                exceptions_raised.append(exception_raised)
            else:
                control_writer_interface.write_line(
                    self._paint(f"Application finished successfully", self.GREEN_COLOR))
                log.info(f"Application finished successfully")
        self._print_raised_exceptions(control_writer_interface, exceptions_raised, log)

    def _print_raised_exceptions(self, control_writer_interface, exceptions_raised, log):
        if exceptions_raised:
            control_writer_interface.write_line(
                self._paint(f"Number of exceptions raised: {len(exceptions_raised)}", self.RED_COLOR))
            control_writer_interface.write_line(
                self._paint(f"Exceptions raised during program execution:", self.RED_COLOR))
            for exception_raised in exceptions_raised:
                control_writer_interface.write_line(
                    self._paint(f"\t- Exception raised: {str(exception_raised)} ", self.RED_COLOR))
            log.error(f"Application finished unsuccessfully")
        else:
            control_writer_interface.write_line(self._paint("All executions were successful!", self.GREEN_COLOR))
            log.info(f"All executions were successful")

    def _paint(self, string, color):
        return f"{color}{string}{self.END_COLOR}"

    def _try_run_application(self, parameters, log):
        try:
            self._application.run(parameters)
        except Exception as e:
            log.exception(f"Exception raised while running application")
            return e
