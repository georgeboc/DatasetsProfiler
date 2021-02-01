from processors.processor import Processor
from results.string_results import StringResults
from utils.error import eprint


class StringProcessor(Processor):
    def __init__(self, column_statistics_calculator):
        self._column_statistics_calculator = column_statistics_calculator

    def process(self, column_rdd):
        key_message_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        if key_message_rdd_cached.isEmpty():
            return StringResults()
        count_null = key_message_rdd_cached.filter(lambda row: row[1] is None).count()
        not_null_key_message_rdd_cached = key_message_rdd_cached.filter(lambda row: row[1] is not None).cache()

        message_length_statistics = self._get_message_length_statistics(not_null_key_message_rdd_cached)
        count_blank_spaces_statistics = self._get_count_blank_spaces_statistics(not_null_key_message_rdd_cached)
        words_length_statistics, words_rdd_cached = self._get_words_length_statistics(not_null_key_message_rdd_cached)
        characters_rdd_cached = not_null_key_message_rdd_cached.flatMap(self._character_split).cache()
        count_distinct_characters, count_distinct_strings, count_distinct_words = self._get_count_distincts(
            not_null_key_message_rdd_cached, words_rdd_cached, characters_rdd_cached)
        return StringResults(message_length_statistics=message_length_statistics,
                             count_blank_spaces_statistics=count_blank_spaces_statistics,
                             words_length_statistics=words_length_statistics,
                             count_distinct_messages=count_distinct_strings,
                             count_distinct_words=count_distinct_words,
                             count_distinct_characters=count_distinct_characters,
                             messages_entropy=self._column_statistics_calculator.calculate_entropy(not_null_key_message_rdd_cached),
                             words_entropy=self._column_statistics_calculator.calculate_entropy(words_rdd_cached),
                             characters_entropy=self._column_statistics_calculator.calculate_entropy(characters_rdd_cached),
                             count_null=count_null,
                             count_not_null=not_null_key_message_rdd_cached.count())

    def _get_count_distincts(self, message_rdd_cached, words_rdd_cached, characters_rdd):
        count_distinct_messages = message_rdd_cached.distinct().count()
        count_distinct_words = words_rdd_cached.distinct().count()
        count_distinct_characters = characters_rdd.distinct().count()
        return count_distinct_characters, count_distinct_messages, count_distinct_words

    def _character_split(self, row):
        key, message = row
        return [(key, character) for character in message]

    def _get_words_length_statistics(self, key_message_rdd_cached):
        words_rdd_cached = key_message_rdd_cached.flatMap(self._word_split).cache()
        words_length_rdd = words_rdd_cached.mapValues(lambda word: len(word))
        words_length_statistics = self._column_statistics_calculator.calculate_number_statistics(words_length_rdd)
        return words_length_statistics, words_rdd_cached

    def _word_split(self, row):
        key, message = row
        return [(key, word) for word in message.split(' ')]

    def _get_message_length_statistics(self, key_message_rdd_cached):
        message_length_rdd = key_message_rdd_cached.mapValues(lambda message: len(message))
        message_length_statistics = self._column_statistics_calculator.calculate_number_statistics(message_length_rdd)
        return message_length_statistics

    def _get_count_blank_spaces_statistics(self, key_message_rdd_cached):
        count_blank_spaces_rdd = key_message_rdd_cached.mapValues(lambda message: message.count(' '))
        count_blank_spaces_statistics = self._column_statistics_calculator.calculate_number_statistics(count_blank_spaces_rdd)
        return count_blank_spaces_statistics
