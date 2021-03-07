from datasets_profiler.src.configuration.execute_if_flag_is_enabled import execute_if_flag_is_enabled
from datasets_profiler.src.instrumentation.call_tracker import instrument_call
from datasets_profiler.src.processors.processor import Processor
from datasets_profiler.src.results.string_results import StringResults


class StringProcessor(Processor):
    def __init__(self, column_statistics_calculator, call_tracker, processors_operations_flags):
        self._column_statistics_calculator = column_statistics_calculator
        self._call_tracker = call_tracker
        self._processors_operations_flags = processors_operations_flags

    @instrument_call
    def process(self, column_rdd):
        key_message_rdd_cached = column_rdd.map(lambda value: (1, value[0])).cache()
        if key_message_rdd_cached.isEmpty():
            return StringResults()
        messages_rdd_cached = key_message_rdd_cached.filter(lambda row: row[1] is not None).cache()
        words_rdd_cached = messages_rdd_cached.flatMap(self._word_split).cache()
        characters_rdd_cached = messages_rdd_cached.flatMap(self._character_split).cache()
        return StringResults(message_length_statistics=self._get_message_length_statistics(messages_rdd_cached),
                             count_blank_spaces_statistics=self._get_count_blank_spaces_statistics(messages_rdd_cached),
                             words_length_statistics=self._get_words_length_statistics(messages_rdd_cached),
                             count_distinct_messages=self._calculate_distinct_messages_count(messages_rdd_cached),
                             count_distinct_words=self._calculate_distinct_words_count(words_rdd_cached),
                             count_distinct_characters=self._calculate_distinct_characters_count(characters_rdd_cached),
                             messages_entropy=self._calculate_messages_entropy(messages_rdd_cached),
                             words_entropy=self._calculate_words_entropy(words_rdd_cached),
                             characters_entropy=self._calculate_characters_entropy(characters_rdd_cached),
                             count_null=self._calculate_null_rows_count(key_message_rdd_cached),
                             count_not_null=self._calculate_not_null_rows_count(messages_rdd_cached))

    @execute_if_flag_is_enabled("string_processor_calculate_not_null_rows_count_is_enabled")
    @instrument_call
    def _calculate_not_null_rows_count(self, not_null_key_message_rdd_cached):
        return not_null_key_message_rdd_cached.count()

    @execute_if_flag_is_enabled("string_processor_calculate_characters_entropy_is_enabled")
    @instrument_call
    def _calculate_characters_entropy(self, characters_rdd_cached):
        return self._column_statistics_calculator.calculate_entropy(characters_rdd_cached)

    @execute_if_flag_is_enabled("string_processor_calculate_words_entropy_is_enabled")
    @instrument_call
    def _calculate_words_entropy(self, words_rdd_cached):
        return self._column_statistics_calculator.calculate_entropy(words_rdd_cached)

    @execute_if_flag_is_enabled("string_processor_calculate_messages_entropy_is_enabled")
    @instrument_call
    def _calculate_messages_entropy(self, not_null_key_message_rdd_cached):
        return self._column_statistics_calculator.calculate_entropy(not_null_key_message_rdd_cached)

    @execute_if_flag_is_enabled("string_processor_calculate_null_rows_count_is_enabled")
    @instrument_call
    def _calculate_null_rows_count(self, key_message_rdd_cached):
        return key_message_rdd_cached.filter(lambda row: row[1] is None).count()

    @execute_if_flag_is_enabled("string_processor_calculate_distinct_messages_count_is_enabled")
    @instrument_call
    def _calculate_distinct_messages_count(self, message_rdd_cached):
        return message_rdd_cached.distinct().count()

    @execute_if_flag_is_enabled("string_processor_calculate_distinct_words_count_is_enabled")
    @instrument_call
    def _calculate_distinct_words_count(self, words_rdd_cached):
        return words_rdd_cached.distinct().count()

    @execute_if_flag_is_enabled("string_processor_calculate_distinct_characters_count_is_enabled")
    @instrument_call
    def _calculate_distinct_characters_count(self, characters_rdd):
        return characters_rdd.distinct().count()

    @execute_if_flag_is_enabled("string_processor_get_words_length_statistics_is_enabled")
    @instrument_call
    def _get_words_length_statistics(self, words_rdd_cached):
        words_length_rdd_cached = words_rdd_cached.mapValues(lambda word: len(word)).cache()
        words_length_statistics = self._column_statistics_calculator.calculate_number_statistics(words_length_rdd_cached)
        return words_length_statistics

    @execute_if_flag_is_enabled("string_processor_get_message_length_statistics_is_enabled")
    @instrument_call
    def _get_message_length_statistics(self, key_message_rdd_cached):
        message_length_rdd_cached = key_message_rdd_cached.mapValues(lambda message: len(message)).cache()
        message_length_statistics = self._column_statistics_calculator.calculate_number_statistics(message_length_rdd_cached)
        return message_length_statistics

    @execute_if_flag_is_enabled("string_processor_get_count_blank_spaces_statistics_is_enabled")
    @instrument_call
    def _get_count_blank_spaces_statistics(self, key_message_rdd_cached):
        count_blank_spaces_rdd_cached = key_message_rdd_cached.mapValues(lambda message: message.count(' ')).cache()
        count_blank_spaces_statistics = self._column_statistics_calculator.calculate_number_statistics(count_blank_spaces_rdd_cached)
        return count_blank_spaces_statistics

    def _character_split(self, row):
        key, message = row
        return [(key, character) for character in message]

    def _word_split(self, row):
        key, message = row
        return [(key, word) for word in message.split(' ')]
