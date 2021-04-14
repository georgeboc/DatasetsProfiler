from typing import TypedDict

from pyspark.sql import DataFrame


class CountValuesResults(TypedDict):
    data_frame: DataFrame
    distinct_rows_count: int
    total_rows_count: int
    is_unique: bool
