from dataclasses import dataclass

from datasets_evaluation.src.results.number_statistics import NumberStatistics


@dataclass
class StringResults:
    message_length_statistics: NumberStatistics = None
    count_blank_spaces_statistics: NumberStatistics = None
    words_length_statistics: NumberStatistics = None
    count_not_null: int = 0
    count_null: int = 0
    count_distinct_messages: int = 0
    count_distinct_words: int = 0
    count_distinct_characters: int = 0
    messages_entropy: float = None
    words_entropy: float = None
    characters_entropy: float = None
