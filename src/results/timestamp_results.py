from dataclasses import dataclass
from datetime import datetime

from results.number_statistics import NumberStatistics


@dataclass
class TimestampResults:
    oldest_date: datetime
    newest_date: datetime
    consecutive_rows_diff_in_seconds: NumberStatistics
    count: int
    count_distinct: int
