from pathlib import Path

from datasets_evaluation.src.utils.files import get_full_path


class RepetitiveExecution:
    GREEN_COLOR = "\033[92m"
    BLUE_COLOR = "\033[94m"
    CYAN_COLOR = "\033[36m"
    RED_COLOR = "\033[0;31m"
    END_COLOR = "\033[0m"
    DEFAULT_PARAMETERS_LIST_FILE_PATH = Path("parameters_lists/parameters_list.json")

    def __init__(self, application, parameters_reader, interface_providers):
        self._application = application
        self._parameters_reader = parameters_reader
        self._interface_providers = interface_providers

    def run(self):
        control_writer_interface = self._interface_providers.control_writer_interface()
        control_reader_interface = self._interface_providers.control_reader_interface()

        control_writer_interface.write_line(f"Introduce path of parameters_list file (default is {self.DEFAULT_PARAMETERS_LIST_FILE_PATH}): ")
        parameters_list_file_path = control_reader_interface.read_line()
        data_reader_interface = self._interface_providers.data_reader_interface(parameters_list_file_path
                                                                                if parameters_list_file_path
                                                                                else get_full_path(self.DEFAULT_PARAMETERS_LIST_FILE_PATH))

        exceptions_raised = []
        parameters_list = self._parameters_reader.get_parameters_list(data_reader_interface)
        for parameters in parameters_list:
            control_writer_interface.write_line(
                self._paint(f"Running application for {parameters.input_path}", self.BLUE_COLOR))
            exception_raised = self._try_run_application(parameters)
            if exception_raised:
                exceptions_raised.append(exception_raised)
        self._print_raised_exceptions(control_writer_interface, exceptions_raised)

    def _print_raised_exceptions(self, control_writer_interface, exceptions_raised):
        if exceptions_raised:
            control_writer_interface.write_line(
                self._paint(f"Number of exceptions raised: {len(exceptions_raised)}", self.RED_COLOR))
            control_writer_interface.write_line(
                self._paint(f"Exceptions raised during program execution:", self.RED_COLOR))
            for exception_raised in exceptions_raised:
                control_writer_interface.write_line(
                    self._paint(f"\t- Exception raised: {str(exception_raised)} ", self.RED_COLOR))
        else:
            control_writer_interface.write_line(self._paint("Jobs completed successfully!", self.GREEN_COLOR))

    def _paint(self, string, color):
        return f"{color}{string}{self.END_COLOR}"

    def _try_run_application(self, parameters):
        try:
            self._application.run(parameters)
        except Exception as e:
            return e
