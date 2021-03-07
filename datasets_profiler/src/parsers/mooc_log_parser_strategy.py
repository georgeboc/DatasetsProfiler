from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType

from datasets_profiler.src.parsers.parser_commons import NULLABLE
from datasets_profiler.src.parsers.parser_strategy import ParserStrategy


class MoocLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        parsed_parameters = [None, ]*9
        for parameter_index, parameter in enumerate(self._parser_commons.nullify_missing_fields(row_string.split(','))):
            parsed_parameters[parameter_index] = parameter
        enroll_id_s, username_s, course_id_s, session_id_s, action_s, object_s, _, date_time_s, truth_s = parsed_parameters
        enroll_id = int(enroll_id_s) if enroll_id_s else None
        username = int(username_s) if username_s else None
        date_time = datetime.strptime(date_time_s, '%Y-%m-%dT%H:%M:%S') if date_time_s else None
        truth = int(truth_s) if truth_s else None
        return enroll_id, username, course_id_s, session_id_s, action_s, object_s, date_time, truth

    def get_schema(self):
        return [
            ("EnrollId", IntegerType(), NULLABLE),
            ("Username", IntegerType(), NULLABLE),
            ("CourseId", StringType(), NULLABLE),
            ("SessionId", StringType(), NULLABLE),
            ("Action", StringType(), NULLABLE),
            ("Object", StringType(), NULLABLE),
            ("DateTime", TimestampType(), NULLABLE),
            ("Truth", IntegerType(), NULLABLE)
        ]

    def is_header_present(self):
        return True