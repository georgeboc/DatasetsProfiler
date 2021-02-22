from logging import getLogger


class RepetitiveExecution:
    GREEN_COLOR = "\033[92m"
    BLUE_COLOR = "\033[94m"
    CYAN_COLOR = "\033[36m"
    RED_COLOR = "\033[0;31m"
    END_COLOR = "\033[0m"
    DEFAULT_LOG_FOLDER = "/files/datasets_evaluation/log"

    def __init__(self, application, parameters_reader, interface_providers, logger_initializer):
        self._application = application
        self._parameters_reader = parameters_reader
        self._interface_providers = interface_providers
        self._logger_initializer = logger_initializer

    def run(self):
        control_writer_interface = self._interface_providers.control_writer_interface()
        control_reader_interface = self._interface_providers.control_reader_interface()

        control_writer_interface.write_line(f"Introduce path of parameters_list file: ")
        parameters_list_file_path = control_reader_interface.read_line()

        control_writer_interface.write_line(f"Introduce path of log directory (default is {self.DEFAULT_LOG_FOLDER}): ")
        log_directory_path = control_reader_interface.read_line()

        self._logger_initializer.initialize(log_directory_path if log_directory_path else self.DEFAULT_LOG_FOLDER)
        logger = getLogger(__name__)

        data_reader_interface = self._interface_providers.data_reader_interface(parameters_list_file_path)
        logger.info("All instances were fully initialized")

        exceptions_raised = []
        logger.info("Getting parameters list")
        parameters_list = self._parameters_reader.get_parameters_list(data_reader_interface)
        for parameters in parameters_list:
            control_writer_interface.write_line(
                self._paint(f"Running application with input path: {parameters.input_path}", self.BLUE_COLOR))
            logger.info(f"Running application with following parameters: {parameters}")
            exception_raised = self._try_run_application(parameters, logger)
            if exception_raised:
                control_writer_interface.write_line(
                    self._paint(f"Application finished with errors: {exception_raised}", self.RED_COLOR))
                logger.error(f"Application finished with errors: {exception_raised}")
                exceptions_raised.append(exception_raised)
            else:
                control_writer_interface.write_line(
                    self._paint(f"Application finished successfully", self.GREEN_COLOR))
                logger.error(f"Application finished successfully")
        self._print_raised_exceptions(control_writer_interface, exceptions_raised, logger)

    def _print_raised_exceptions(self, control_writer_interface, exceptions_raised, logger):
        if exceptions_raised:
            control_writer_interface.write_line(
                self._paint(f"Number of exceptions raised: {len(exceptions_raised)}", self.RED_COLOR))
            control_writer_interface.write_line(
                self._paint(f"Exceptions raised during program execution:", self.RED_COLOR))
            for exception_raised in exceptions_raised:
                control_writer_interface.write_line(
                    self._paint(f"\t- Exception raised: {str(exception_raised)} ", self.RED_COLOR))
            logger.error(f"Application finished unsuccessfully")
        else:
            control_writer_interface.write_line(self._paint("All executions were successfully!", self.GREEN_COLOR))
            logger.info(f"Application finished successfully")

    def _paint(self, string, color):
        return f"{color}{string}{self.END_COLOR}"

    def _try_run_application(self, parameters, logger):
        try:
            self._application.run(parameters)
        except Exception as e:
            logger.exception(f"Exception raised while running application")
            return e
