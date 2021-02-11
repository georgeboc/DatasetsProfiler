from dataclasses import dataclass

from datasets_evaluation.src.results.number_statistics import NumberStatistics


@dataclass
class NumericResults:
    number_statistics: NumberStatistics = None
    count_not_null: int = 0
    count_null: int = 0
    count_distinct: int = 0
    entropy: float = None
