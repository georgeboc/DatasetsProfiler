from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType

from datasets_evaluation.src.parsers.parser_commons import NULLABLE
from datasets_evaluation.src.parsers.parser_strategy import ParserStrategy


class AdClickOnTaobaoLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        user_s, timestamp_s, ad_group_id_s, pid_s, non_clk_s, clk_s = \
            self._parser_commons.nullify_missing_fields(row_string.split(','))
        date_time = datetime.fromtimestamp(int(timestamp_s)) if timestamp_s else None
        user = int(user_s) if user_s else None
        add_group_id = int(ad_group_id_s) if ad_group_id_s else None
        non_clk = int(non_clk_s) if non_clk_s else None
        clk = int(clk_s) if clk_s else None
        return user, date_time, add_group_id, pid_s, non_clk, clk

    def get_schema(self):
        return [
            ("User", IntegerType(), NULLABLE),
            ("DateTime", TimestampType(), NULLABLE),
            ("AdGroupId", IntegerType(), NULLABLE),
            ("PID", StringType(), NULLABLE),
            ("NonClk", IntegerType(), NULLABLE),
            ("Clk", IntegerType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
