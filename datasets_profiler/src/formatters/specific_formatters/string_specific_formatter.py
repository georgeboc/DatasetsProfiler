from typing import Any


class StringSpecificFormatter:
    def format(self, element):
        return str(element)

    def get_source_type(self):
        return Any
