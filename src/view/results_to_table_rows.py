from dataclasses import asdict
from functools import reduce
from collections import OrderedDict


class ResultsToTableRows:
    def get_table_rows(self, results):
        flattened_dictionaries = self._flatten_results_into_dictionaries(results)
        dictionary = self._merge_dictionaries(flattened_dictionaries)
        return map(lambda tuple: [tuple[0], *tuple[1]], dictionary.items())

    def _flatten_results_into_dictionaries(self, results):
        dictionaries = [asdict(result) for result in results]
        flattened_dictionaries = []
        for dictionary in dictionaries:
            flattened_dictionary = OrderedDict()
            for key, value in dictionary.items():
                if type(value) == dict:
                    for key_sub_dict, value_sub_dict in value.items():
                        flattened_dictionary[f"{key}_{key_sub_dict}"] = value_sub_dict
                else:
                    flattened_dictionary[key] = value
            flattened_dictionaries.append(flattened_dictionary)
        return flattened_dictionaries

    def _merge_dictionaries(self, dictionaries):
        keys_super_set = self._get_keys_super_set(dictionaries)
        result = OrderedDict()
        for dictionary in dictionaries:
            for key in keys_super_set:
                result.setdefault(key, []).append(dictionary[key] if key in dictionary else '-')
        return result

    def _get_keys_super_set(self, dictionaries):
        unordered_key_sets = [set(dictionary.keys()) for dictionary in dictionaries]
        unordered_remaining_distinct_keys = reduce(lambda first_set, second_set: first_set.union(second_set), unordered_key_sets)
        ordered_key_lists = [list(dictionary.keys()) for dictionary in dictionaries]
        result = []
        for ordered_key_list in ordered_key_lists:
            for key in ordered_key_list:
                if key in unordered_remaining_distinct_keys:
                    result.append(key)
                    unordered_remaining_distinct_keys.remove(key)
        return result
