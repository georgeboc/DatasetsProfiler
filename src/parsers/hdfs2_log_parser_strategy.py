import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType

from parsers.parser_commons import NULLABLE
from parsers.parser_strategy import ParserStrategy


class HDFS2LogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        tabbed_row_spaces = re.sub(' ', '\t', row_string, count=3)
        tabbed_row_header_content_separator = re.sub(': ', '\t', tabbed_row_spaces, count=1)
        date_s, time_s, level_s, component_s, content_s = \
            self._parser_commons.nullify_missing_fields(tabbed_row_header_content_separator.split('\t'))
        date_time = datetime.strptime(f"{date_s} {time_s}", '%Y-%m-%d %H:%M:%S,%f') if date_s and time_s else None
        return date_time, level_s, component_s, content_s

    def get_schema(self):
        return [
            ("DateTime", TimestampType(), NULLABLE),
            ("Level", StringType(), NULLABLE),
            ("Component", StringType(), NULLABLE),
            ("Content", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
