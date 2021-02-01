import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, IntegerType

from parsers.parser_commons import NULLABLE
from parsers.parser_strategy import ParserStrategy


class ThunderbirdLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        tabbed_row_spaces = re.sub(' ', '\t', row_string, count=8)
        fields = self._parser_commons.nullify_missing_fields(tabbed_row_spaces.split('\t'))
        alert_s = ''
        if len(fields) == 9:
            alert_s, timestamp_s, date_s, node_s, month_s, day_s, time_s, uri_s, content_s = fields
        elif len(fields) == 8:
            timestamp_s, date_s, node_s, month_s, day_s, time_s, uri_s, content_s = fields
        else:
            raise ValueError("Number of fields is not 8 or 9")
        timestamp_parsed = datetime.fromtimestamp(int(timestamp_s)) if timestamp_s else None
        date_parsed = datetime.strptime(date_s, '%Y.%m.%d') if date_s else None
        date_time_parsed = datetime.strptime(f"{month_s} {day_s} {time_s}",
                                             '%b %d %H:%M:%S') if month_s and day_s and time_s else None
        alert = int(alert_s != '-')
        return alert, timestamp_parsed, date_parsed, node_s, date_time_parsed, uri_s, content_s

    def get_schema(self):
        return [
            ("IsAlertMessage?", IntegerType(), NULLABLE),
            ("Timestamp", TimestampType(), NULLABLE),
            ("Date", TimestampType(), NULLABLE),
            ("Node", StringType(), NULLABLE),
            ("DateTime", TimestampType(), NULLABLE),
            ("URI", StringType(), NULLABLE),
            ("Content", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
