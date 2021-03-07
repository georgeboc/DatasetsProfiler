from collections import OrderedDict
from functools import reduce


class ResultsToTableRows:
    WORD_SEPARATOR = '_'
    NOT_APPLICABLE = '-'
    SECTION_SEPARATOR = "******"
    EMPTY = ''
    DESCRIPTOR_TITLE_LEN = 1

    def get_table_rows(self, results):
        flattened_results = list((result.type, self._flatten_dictionary(result.dictionary)) for result in results)
        type_multidict = self._create_multidict(flattened_results)
        types_dict = dict((type_element, self._merge_dictionaries(dictionaries))
                          for type_element, dictionaries in type_multidict.items())
        result_types = [result.type for result in results]
        return self._render_rows_by_types(types_dict, result_types)

    def _create_multidict(self, key_value_iterable):
        type_multidict = OrderedDict()
        for key, value in key_value_iterable:
            type_multidict.setdefault(key, []).append(value)
        return type_multidict

    def _flatten_dictionary(self, dictionary, prefix=None):
        result_dictionary = OrderedDict()
        for key, value in dictionary.items():
            if type(value) == dict:
                new_prefix = self._add_prefix(prefix, key)
                result_dictionary = {**result_dictionary, **self._flatten_dictionary(value, prefix=new_prefix)}
            else:
                result_dictionary[self._add_prefix(prefix, key)] = value
        return result_dictionary

    def _add_prefix(self, prefix, key):
        if prefix is None:
            return key
        return prefix + self.WORD_SEPARATOR + key

    def _merge_dictionaries(self, dictionaries):
        keys_super_set = self._get_keys_super_set(dictionaries)
        result = OrderedDict()
        for dictionary in dictionaries:
            for key in keys_super_set:
                result.setdefault(key, []).append(dictionary[key] if key in dictionary else self.NOT_APPLICABLE)
        return result

    def _get_keys_super_set(self, dictionaries):
        return sorted(reduce(lambda set1, set2: set1.union(set2), [set(dictionary.keys()) for dictionary in dictionaries]))

    def _render_rows_by_types(self, types_dict, result_types):
        types_position = self._create_multidict([(result_type, position) for position, result_type in enumerate(result_types)])
        attributes_count = len(result_types)
        rows = []
        for type_element, descriptors in types_dict.items():
            if len(result_types) > 1:
                rows.append(self._get_header_row(type_element, attributes_count))
            rows.extend(self._dict_to_table_rows_with_position(descriptors, types_position[type_element], attributes_count))
        return rows

    def _dict_to_table_rows_with_position(self, descriptors, positions, columns_count):
        rows = []
        for descriptor, values in descriptors.items():
            row = [self.NOT_APPLICABLE] * columns_count
            for i in range(len(positions)):
                row[positions[i]] = values[i]
            rows.append([self._prettify_descriptor(descriptor), *row])
        return rows

    def _get_header_row(self, title, columns_count):
        row = [self.SECTION_SEPARATOR] * (self.DESCRIPTOR_TITLE_LEN + columns_count)
        row[0] = title
        return row

    def _prettify_descriptor(self, descriptor):
        return descriptor.capitalize().replace(self.WORD_SEPARATOR, ' ')
