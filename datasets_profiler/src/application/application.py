from logging import getLogger

LOG = getLogger(__name__)


class Application:
    def __init__(self, use_case_providers):
        self._use_case_providers = use_case_providers

    def run(self, parameters):
        use_case = self._use_case_providers.get_use_case(parameters.use_case)
        use_case.execute(parameters)
