from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType

from datasets_evaluation.src.parsers.parser_commons import NULLABLE
from datasets_evaluation.src.parsers.parser_strategy import ParserStrategy


class SeattleCobanLogParserStreategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        file_name_s, log_time_s, user_id_s, act_code_s, id_s, sent_s = \
            self._parser_commons.nullify_missing_fields(row_string.split(','))
        log_time = datetime.strptime(log_time_s, '%Y-%m-%dT%H:%M:%S.%f') if log_time_s else None
        id = int(id_s) if id_s else None
        sent = int(sent_s) if sent_s else None
        return file_name_s, log_time, user_id_s, act_code_s, id, sent

    def get_schema(self):
        return [
            ("fileName", StringType(), NULLABLE),
            ("logTime", TimestampType(), NULLABLE),
            ("userID", StringType(), NULLABLE),
            ("actCode", StringType(), NULLABLE),
            ("ID", IntegerType(), NULLABLE),
            ("sent", IntegerType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
