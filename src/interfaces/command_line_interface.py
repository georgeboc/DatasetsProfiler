import sys


class CommandLineInterface:
    def get_path_or_default(self, default):
        return self._input_or_default(f"Introduce dataset filename in resources folder to describe: (default {default}) ", default)

    def get_schema_transformer_or_default(self, default):
        return self._input_or_default(f"Introduce schema transformer class name to use: (default {default}) ", default)

    def print_string(self, *string):
        print(*string)

    def _input_or_default(self, input_prompt, default):
        sys.stderr.write(input_prompt)
        input_text = input()
        if not input_text:
            return default
        return input_text
