import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, LongType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class TestLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        tabbed_row = re.sub(': ', '\t', row_string, count=1)
        tabbed_row_with_spaces = re.sub(' ', '\t', tabbed_row)
        date_s, time_s, log_level_s, class_name_s, message_s = \
            self._parser_commons.nullify_missing_fields(tabbed_row_with_spaces.split('\t'))
        date_time = datetime.strptime(f"{date_s} {time_s}", '%Y-%m-%d %H:%M:%S,%f') if date_s and time_s else None
        message = int(message_s)
        return date_time, log_level_s, class_name_s, message

    def get_schema(self):
        return [
            ("DateTime", TimestampType(), NULLABLE),
            ("Level", StringType(), NULLABLE),
            ("Component", StringType(), NULLABLE),
            ("Content", LongType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
