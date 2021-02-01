from dataclasses import dataclass
from datetime import datetime

from results.number_statistics import NumberStatistics


@dataclass
class TimestampResults:
    oldest_date: datetime
    newest_date: datetime
    count: int
    count_distinct: int
    timestamp_entropy: float
    delta_time_in_seconds_statistics: NumberStatistics
    delta_time_in_seconds_entropy: float
