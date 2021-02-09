def execute_if_flag_is_enabled(configuration_name):
    def function_wrapper(function):
        def wrapper(self, *args, **kwargs):
            if getattr(self._processors_operations_flags, configuration_name):
                return function(self, *args, **kwargs)
        return wrapper
    return function_wrapper
