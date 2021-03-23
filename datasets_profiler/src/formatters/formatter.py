from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class Result:
    type: str = None
    column_name: str = None
    dictionary: dict = None


class DictionaryFormatter:
    def format_dictionary(self, dictionary, formatter):
        formatted_dictionary = {}
        for key, value in dictionary.items():
            if type(value) == dict:
                formatted_dictionary[key] = self.format_dictionary(value, formatter)
            elif formatter.get_source_type() == Any or type(value) == formatter.get_source_type():
                formatted_dictionary[key] = formatter.format(value)
            else:
                formatted_dictionary[key] = value
        return formatted_dictionary


class Formatter:
    def __init__(self, dictionary_formatter):
        self._dictionary_formatter = dictionary_formatter

    def format(self, column_name_stats_dict, specific_formatters=None):
        formatted_results = [Result(type=stats.__class__.__name__,
                                    column_name=column_name,
                                    dictionary=asdict(stats) if not isinstance(stats, dict) else stats)
                             for column_name, stats in column_name_stats_dict.items()]
        if specific_formatters is None:
            return formatted_results

        return [Result(type=formatted_result.type,
                       column_name=formatted_result.column_name,
                       dictionary=self._dictionary_formatter.format_dictionary(formatted_result.dictionary,
                                                                               specific_formatters[i]))
                for i, formatted_result in enumerate(formatted_results)]
