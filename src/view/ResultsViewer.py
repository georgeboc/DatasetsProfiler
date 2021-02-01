from dataclasses import asdict
from functools import reduce
from prettytable import PrettyTable


class ResultsViewer:
    def print_results(self, results, attributes_names):
        table = PrettyTable(['', *attributes_names])
        dictionaries = [asdict(result) for result in results]
        dictionary = self.merge_dictionaries(dictionaries)
        table_rows = map(lambda tuple: [tuple[0], *tuple[1]], dictionary.items())
        table.add_rows(table_rows)
        print(table)

    def merge_dictionaries(self, dictionaries):
        keys_super_set = self._get_keys_super_set(dictionaries)
        result = {}
        for dictionary in dictionaries:
            for key in keys_super_set:
                result.setdefault(key, []).append(dictionary[key] if key in dictionary else '-')
        return result

    def _get_keys_super_set(self, dictionaries):
        keys_sets = [set(dictionary.keys()) for dictionary in dictionaries]
        return reduce(lambda first_set, second_set: first_set.union(second_set), keys_sets)

