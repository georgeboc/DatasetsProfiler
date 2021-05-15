from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType, FloatType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class AdClickOnTaobaoAdFeatureLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        adgroup_id_s, category_id_s, campaign_id_s, customer_s, brand_s, price_s = \
            self._parser_commons.nullify_missing_fields(row_string.split(','))
        adgroup_id = int(adgroup_id_s) if adgroup_id_s else None
        category_id = int(category_id_s) if adgroup_id_s else None
        campaign_id = int(campaign_id_s) if adgroup_id_s else None
        customer = int(customer_s) if adgroup_id_s else None
        brand = int(brand_s) if adgroup_id_s else None
        price = float(price_s) if adgroup_id_s else None
        return adgroup_id, category_id, campaign_id, customer, brand, price

    def get_schema(self):
        return [
            ("AdGroupId", IntegerType(), NULLABLE),
            ("CategoryId", IntegerType(), NULLABLE),
            ("CampaignId", IntegerType(), NULLABLE),
            ("Customer", IntegerType(), NULLABLE),
            ("Brand", IntegerType(), NULLABLE),
            ("Price", FloatType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
