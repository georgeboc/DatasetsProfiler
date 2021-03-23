from typing import TypedDict

from pyspark.sql import DataFrame


class CountValuesResults(TypedDict):
    data_frame: DataFrame
    rows_count: int
    is_unique: bool
