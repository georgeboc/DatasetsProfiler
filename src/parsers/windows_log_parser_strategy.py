import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType

from parsers.parser_commons import NULLABLE
from parsers.parser_strategy import ParserStrategy


class WindowsLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        clean_strange_characters = re.sub('\ufeff', '', row_string, count=1)
        tabbed_row_date_time = re.sub(' ', '\t', clean_strange_characters, count=1)
        tabbed_row_date_loglevel = re.sub(', ', '\t', tabbed_row_date_time, count=1)
        tabbed_row = re.sub('  +', '\t', tabbed_row_date_loglevel, count=2)
        date_s, time_s, log_level_s, component_s, message_s = \
            self._parser_commons.nullify_missing_fields(tabbed_row.split('\t'))
        date_time = datetime.strptime(f"{date_s} {time_s}", '%Y-%m-%d %H:%M:%S') if date_s and time_s else None
        return date_time, log_level_s, component_s, message_s

    def get_schema(self):
        return [
            ("DateTime", TimestampType(), NULLABLE),
            ("Level", StringType(), NULLABLE),
            ("Component", StringType(), NULLABLE),
            ("Content", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
