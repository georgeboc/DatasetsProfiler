from dataclasses import dataclass

from results.number_statistics import NumberStatistics


@dataclass
class StringResults:
    message_length_statistics: NumberStatistics
    count_blank_spaces_statistics: NumberStatistics
    words_length_statistics: NumberStatistics
    count_distinct_messages: int
    count_distinct_words: int
    count_distinct_characters: int
