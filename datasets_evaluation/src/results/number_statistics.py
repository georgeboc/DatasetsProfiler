from dataclasses import dataclass


@dataclass
class NumberStatistics:
    min: float
    max: float
    average: float
    variance: float
    standard_deviation: float
