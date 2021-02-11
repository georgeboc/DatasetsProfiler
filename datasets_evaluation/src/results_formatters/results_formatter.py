from dataclasses import asdict
from typing import Any

from datasets_evaluation.src.results_formatters.result import Result


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


class ResultsFormatter:
    def __init__(self, dictionary_formatter):
        self._dictionary_formatter = dictionary_formatter

    def format_results(self, results, formatters):
        type_dictionaries = [Result(type=result.__class__.__name__, dictionary=asdict(result)) for result in results]
        if formatters is None:
            return type_dictionaries
        return [Result(type=type_dictionaries[i].type,
                       dictionary=self._dictionary_formatter.format_dictionary(type_dictionaries[i].dictionary, formatters[i]))
                for i in range(len(type_dictionaries))]
