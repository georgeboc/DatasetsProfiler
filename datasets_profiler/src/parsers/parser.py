from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType
from dataclasses import dataclass
from logging import getLogger
from typing import Any


LOG = getLogger(__name__)


@dataclass
class ParserStatistics:
    original_rows_count: int
    filtered_out_rows_count: int
    lost_rows_percentage: float


@dataclass
class ParserResult:
    parsed_data_frame: Any
    parser_statistics: ParserStatistics


class Parser:
    def __init__(self, parser_strategy, spark_configuration):
        self._parser_strategy = parser_strategy
        self._spark_configuration = spark_configuration

    def parse(self, source_rdd):
        source_rdd_cached = source_rdd.cache()
        skip_if_present_header = self._filter_out_header(source_rdd_cached) \
            if self._parser_strategy.is_header_present() \
            else source_rdd_cached
        split_rdd = skip_if_present_header.map(self._parser_exception_wrapper(self._parser_strategy.parse))
        parseable_rows_rdd_cached = split_rdd.filter(bool).cache()
        return ParserResult(parsed_data_frame=self._assign_schema(parseable_rows_rdd_cached,
                                                                  self._parser_strategy.get_schema()),
                            parser_statistics=self._gather_parser_statistics(parseable_rows_rdd_cached,
                                                                             source_rdd_cached))

    def _filter_out_header(self, rdd_cached):
        header = rdd_cached.first()
        return rdd_cached.filter(lambda row: row != header)

    def _parser_exception_wrapper(self, function):
        def wrapper(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except ValueError:
                LOG.exception(f"Exception in parser for input args={args}, kwargs={kwargs}")
        return wrapper

    def _assign_schema(self, rdd, columns_params: list):
        schema = StructType([StructField(*column_params) for column_params in columns_params])
        spark_session = self._spark_configuration.get_spark_session()
        return spark_session.createDataFrame(rdd, schema)

    def _gather_parser_statistics(self, filter_out_non_parseable_rows_rdd_cached, source_rdd_cached):
        original_rows_count = source_rdd_cached.count()
        filtered_in_rows_count = filter_out_non_parseable_rows_rdd_cached.count()
        filtered_out_rows_count = original_rows_count - filtered_in_rows_count
        return ParserStatistics(original_rows_count=original_rows_count,
                                filtered_out_rows_count=filtered_out_rows_count,
                                lost_rows_percentage=filtered_out_rows_count*100/original_rows_count)
