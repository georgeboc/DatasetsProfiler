import re

from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, IntegerType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class BGLLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        tabbed_row = re.sub(' ', '\t', row_string, count=9)
        fields = self._parser_commons.nullify_missing_fields(tabbed_row.split('\t'))
        alert_s = ''
        if len(fields) == 10:
            alert_s, timestamp_s, date_s, node_s, date_time_s, node_repeat_s, type_s, component_s, level_s, \
                content_s = fields
        elif len(fields) == 9:
            timestamp_s, date_s, node_s, date_time_s, node_repeat_s, type_s, component_s, level_s, content_s = fields
        else:
            raise ValueError("Number of fields is not 9 or 10")
        timestamp_parsed = datetime.fromtimestamp(int(timestamp_s)) if timestamp_s else None
        date_parsed = datetime.strptime(date_s, '%Y.%m.%d') if date_s else None
        date_time_parsed = datetime.strptime(date_time_s, '%Y-%m-%d-%H.%M.%S.%f') if date_time_s else None
        alert = int(alert_s != '-')
        return alert, timestamp_parsed, date_parsed, date_time_parsed, node_s, node_repeat_s, type_s, component_s, \
            level_s, content_s

    def get_schema(self):
        return [
            ("IsAlertMessage?", IntegerType(), NULLABLE),
            ("Timestamp", TimestampType(), NULLABLE),
            ("Date", TimestampType(), NULLABLE),
            ("DateTime", TimestampType(), NULLABLE),
            ("Node", StringType(), NULLABLE),
            ("NodeRepeat", StringType(), NULLABLE),
            ("Type", StringType(), NULLABLE),
            ("Component", StringType(), NULLABLE),
            ("Level", StringType(), NULLABLE),
            ("Content", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return False
