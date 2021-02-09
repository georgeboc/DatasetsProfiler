import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, date_format, to_date

from parsers.parser_commons import NULLABLE
from parsers.parser_strategy import ParserStrategy


class AndroidLogParserStrategy(ParserStrategy):
    DATE_TIME_FORMAT = '%m-%d %H:%M:%S.%f'

    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        tabbed_row_date_time = re.sub(' ', '\t', row_string, count=1)
        tabbed_row_pid_tid = re.sub(' +', '\t', tabbed_row_date_time, count=2)
        tabbed_row_priority = re.sub(' ', '\t', tabbed_row_pid_tid, count=2)
        tabbed_row_tag = re.sub(': ', '\t', tabbed_row_priority, count=1)
        date_s, time_s, pid_s, tid_s, priority_s, tag_s, message_s = \
            self._parser_commons.nullify_missing_fields(tabbed_row_tag.split('\t'))
        date_time = datetime.strptime(f"{date_s} {time_s}", self.DATE_TIME_FORMAT) if date_s and time_s else None
        pid = int(pid_s) if pid_s else None
        tid = int(tid_s) if tid_s else None
        return date_time, pid, tid, priority_s, tag_s, message_s

    def get_schema(self):
        return [
            ("DateTime", TimestampType(), NULLABLE),
            ("PID", IntegerType(), NULLABLE),
            ("TID", IntegerType(), NULLABLE),
            ("Priority", StringType(), NULLABLE),
            ("Tag", StringType(), NULLABLE),
            ("Message", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
