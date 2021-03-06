import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, IntegerType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class HDFS1LogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        tabbed_row_spaces = re.sub(' ', '\t', row_string, count=4)
        tabbed_row_header_content_separator = re.sub(': ', '\t', tabbed_row_spaces, count=1)
        date_s, time_s, pid_s, level_s, component_s, content_s = \
            self._parser_commons.nullify_missing_fields(tabbed_row_header_content_separator.split('\t'))
        date_time = datetime.strptime(f"{date_s} {time_s}", '%y%m%d %H%M%S') if date_s and time_s else None
        pid = int(pid_s)
        return date_time, pid, level_s, component_s, content_s

    def get_schema(self):
        return [
            ("DateTime", TimestampType(), NULLABLE),
            ("Pid", IntegerType(), NULLABLE),
            ("Level", StringType(), NULLABLE),
            ("Component", StringType(), NULLABLE),
            ("Content", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
