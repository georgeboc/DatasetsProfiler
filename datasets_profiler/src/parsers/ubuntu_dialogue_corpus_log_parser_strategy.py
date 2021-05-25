import re
from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class UbuntuDialogueCorpusLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        folder_s, dialogue_id_s, date_s, from_s, to_s, text_s = \
            self._parser_commons.nullify_missing_fields(self._parser_commons.split_by_comma_outside_quotes(row_string))
        folder = int(folder_s) if folder_s else None
        date = datetime.strptime(date_s, '%Y-%m-%dT%H:%M:%S.%fZ') if date_s else None
        return folder, dialogue_id_s, date, from_s, to_s, text_s

    def get_schema(self):
        return [
            ("folder", IntegerType(), NULLABLE),
            ("dialogueID", StringType(), NULLABLE),
            ("date", TimestampType(), NULLABLE),
            ("from", StringType(), NULLABLE),
            ("to", StringType(), NULLABLE),
            ("text", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
