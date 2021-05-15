from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType, FloatType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class AdClickOnTaobaoUserProfileLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        user_id_s, cms_seg_id_s, cms_group_id_s, final_gender_code_s, age_level_s, pvalue_level_s, shopping_level_s,\
            occupation_s, new_user_class_level_s = self._parser_commons.nullify_missing_fields(row_string.split(','))
        user_id = int(user_id_s) if user_id_s else None
        cms_seg_id = int(cms_seg_id_s) if cms_seg_id_s else None
        cms_group_id = int(cms_group_id_s) if cms_group_id_s else None
        final_gender_code = int(final_gender_code_s) if final_gender_code_s else None
        age_level = int(age_level_s) if age_level_s else None
        pvalue_level = int(pvalue_level_s) if pvalue_level_s else None
        shopping_level = int(shopping_level_s) if shopping_level_s else None
        occupation = int(occupation_s) if occupation_s else None
        new_user_class_level = int(new_user_class_level_s) if new_user_class_level_s else None
        return user_id, cms_seg_id, cms_group_id, final_gender_code, age_level, pvalue_level, shopping_level,\
            occupation, new_user_class_level

    def get_schema(self):
        return [
            ("UserId", IntegerType(), NULLABLE),
            ("CmsSegId", IntegerType(), NULLABLE),
            ("CmsGroupId", IntegerType(), NULLABLE),
            ("FinalGenderCode", IntegerType(), NULLABLE),
            ("AgeLevel", IntegerType(), NULLABLE),
            ("PValueLevel", IntegerType(), NULLABLE),
            ("ShoppingLevel", IntegerType(), NULLABLE),
            ("Occupation", IntegerType(), NULLABLE),
            ("NewUserClassLevel", IntegerType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
