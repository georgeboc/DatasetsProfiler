from dataclasses import dataclass

from results.number_statistics import NumberStatistics


@dataclass
class IntegerResults:
    number_statistics: NumberStatistics
    count_distinct: int
    entropy: float
