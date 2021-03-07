from dataclasses import dataclass
from datetime import datetime

from datasets_profiler.src.results.number_statistics import NumberStatistics


@dataclass
class TimestampResults:
    oldest_date: datetime = None
    newest_date: datetime = None
    count_not_null: int = 0
    count_null: int = 0
    count_distinct: int = 0
    timestamp_entropy: float = None
    delta_time_in_seconds_statistics: NumberStatistics = None
    delta_time_in_seconds_entropy: float = None
