from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType

from datasets_evaluation.src.parsers.parser_commons import NULLABLE
from datasets_evaluation.src.parsers.parser_strategy import ParserStrategy


class RecommenderClickLogsSowiportLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        row_with_nulls = row_string.replace("\\N", '')
        recommendation_id_s, recommendation_class_s, cbf_feature_type_s, cbf_feature_count_s, category_s, clicked_s, \
            request_received_s, response_delivered_s, processing_time_s = \
                self._parser_commons.nullify_missing_fields(row_with_nulls.split(';'))
        recommendation_id = int(recommendation_id_s) if recommendation_id_s else None
        cbf_feature_count = int(cbf_feature_count_s) if cbf_feature_count_s else None
        request_received = datetime.strptime(request_received_s, '%Y-%m-%d %H:%M:%S') if request_received_s else None
        response_delivered = datetime.strptime(response_delivered_s, '%Y-%m-%d %H:%M:%S') if response_delivered_s else None
        processing_time = int(processing_time_s) if processing_time_s else None
        return recommendation_id, recommendation_class_s, cbf_feature_type_s, cbf_feature_count, category_s, clicked_s, \
               request_received, response_delivered, processing_time

    def get_schema(self):
        return [
            ("recommendation_id", IntegerType(), NULLABLE),
            ("recommendation_class", StringType(), NULLABLE),
            ("cbf_feature_type", StringType(), NULLABLE),
            ("cbf_feature_count", IntegerType(), NULLABLE),
            ("category", StringType(), NULLABLE),
            ("clicked", StringType(), NULLABLE),
            ("request_received", TimestampType(), NULLABLE),
            ("response_delivered", TimestampType(), NULLABLE),
            ("processingTime", IntegerType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
