from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType, FloatType

from datasets_evaluation.src.parsers.parser_commons import NULLABLE
from datasets_evaluation.src.parsers.parser_strategy import ParserStrategy


class UserLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        mesno_s, date_s, num_25_s, num_50_s, num_75_s, num_985_s, num_100_s, num_unq_s, total_secs_s = \
            self._parser_commons.nullify_missing_fields(row_string.split(','))
        date = datetime.strptime(date_s, '%Y%m%d') if date_s else None
        num_25 = int(num_25_s) if num_25_s else None
        num_50 = int(num_50_s) if num_50_s else None
        num_75 = int(num_75_s) if num_75_s else None
        num_985 = int(num_985_s) if num_985_s else None
        num_100 = int(num_100_s) if num_100_s else None
        num_unq = int(num_unq_s) if num_unq_s else None
        total_secs = float(total_secs_s) if total_secs_s else None
        return mesno_s, date, num_25, num_50, num_75, num_985, num_100, num_unq, total_secs

    def get_schema(self):
        return [
            ("msno", StringType(), NULLABLE),
            ("date", TimestampType(), NULLABLE),
            ("num_25", IntegerType(), NULLABLE),
            ("num_50", IntegerType(), NULLABLE),
            ("num_75", IntegerType(), NULLABLE),
            ("num_985", IntegerType(), NULLABLE),
            ("num_100", IntegerType(), NULLABLE),
            ("num_unq", IntegerType(), NULLABLE),
            ("total_secs", FloatType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
