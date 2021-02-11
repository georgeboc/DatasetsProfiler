from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, TimestampType

from datasets_evaluation.src.parsers.parser_commons import NULLABLE
from datasets_evaluation.src.parsers.parser_strategy import ParserStrategy


class ObamaVisitorLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        name_last_s, name_first_s, name_mid_s, uin_s, bdgnbr_s, type_of_access_s, toa_s, poa_s, tod_s, pod_s,\
            appt_made_date_s, appt_start_date_s, appt_end_date_s, appt_cancel_date_s, total_people_s, last_update_by_id_s,\
            post_s, last_entry_date_s, terminal_suffix_s, visitee_namelast_s, visitee_namefirst_s, meeting_loc_s,\
            meeting_room_s, caller_name_last_s, caller_name_first_s, caller_room_s, description_s, release_date_s = \
            self._parser_commons.nullify_missing_fields(row_string.split(','))
        bdgnbr = int(bdgnbr_s) if bdgnbr_s else None
        toa = self._parse_datetime_with_multiple_formats(toa_s)
        tod = self._parse_datetime_with_multiple_formats(tod_s)
        appt_made_date = datetime.strptime(appt_made_date_s, "%m/%d/%Y %I:%M:%S %p") if appt_made_date_s else None
        appt_start_date = self._parse_datetime_with_multiple_formats(appt_start_date_s)
        appt_end_date = self._parse_datetime_with_multiple_formats(appt_end_date_s)
        appt_cancel_date = self._parse_datetime_with_multiple_formats(appt_cancel_date_s)
        total_people = int(total_people_s) if total_people_s else None
        last_entry_date = self._parse_datetime_with_multiple_formats(last_entry_date_s)
        release_date = datetime.strptime(release_date_s, "%m/%d/%Y %I:%M:%S %p %z") if release_date_s else None
        return name_last_s, name_first_s, name_mid_s, uin_s, bdgnbr, type_of_access_s, toa, poa_s, tod, pod_s,\
            appt_made_date, appt_start_date, appt_end_date, appt_cancel_date, total_people, last_update_by_id_s,\
            post_s, last_entry_date, terminal_suffix_s, visitee_namelast_s, visitee_namefirst_s, meeting_loc_s,\
            meeting_room_s, caller_name_last_s, caller_name_first_s, caller_room_s, description_s, release_date

    def get_schema(self):
        return [
            ("NAMELAST", StringType(), NULLABLE),
            ("NAMEFIRST", StringType(), NULLABLE),
            ("NAMEMID", StringType(), NULLABLE),
            ("UIN", StringType(), NULLABLE),
            ("BDGNBR", IntegerType(), NULLABLE),
            ("TYPE_OF_ACCESS", StringType(), NULLABLE),
            ("TOA", TimestampType(), NULLABLE),
            ("POA", StringType(), NULLABLE),
            ("TOD", TimestampType(), NULLABLE),
            ("POD", StringType(), NULLABLE),
            ("APPT_MADE_DATE", TimestampType(), NULLABLE),
            ("APPT_START_DATE", TimestampType(), NULLABLE),
            ("APPT_END_DATE", TimestampType(), NULLABLE),
            ("APPT_CANCEL_DATE", TimestampType(), NULLABLE),
            ("Total_People", IntegerType(), NULLABLE),
            ("LAST_UPDATEDBY", StringType(), NULLABLE),
            ("POST", StringType(), NULLABLE),
            ("LastEntryDate", TimestampType(), NULLABLE),
            ("TERMINAL_SUFFIX", StringType(), NULLABLE),
            ("visitee_namelast", StringType(), NULLABLE),
            ("visitee_namefirst", StringType(), NULLABLE),
            ("MEETING_LOC", StringType(), NULLABLE),
            ("MEETING_ROOM", StringType(), NULLABLE),
            ("CALLER_NAME_LAST", StringType(), NULLABLE),
            ("CALLER_NAME_FIRST", StringType(), NULLABLE),
            ("CALLER_ROOM", StringType(), NULLABLE),
            ("Description", StringType(), NULLABLE),
            ("RELEASE_DATE", TimestampType(), NULLABLE)
        ]

    def is_header_present(self):
        return True

    def _parse_datetime_with_multiple_formats(self, date_time_string):
        if date_time_string is None:
            return
        try:
            return datetime.strptime(date_time_string, '%m/%d/%y')
        except ValueError:
            try:
                return datetime.strptime(date_time_string, '%m/%d/%Y')
            except ValueError:
                try:
                    return datetime.strptime(date_time_string, '%m/%d/%y %H:%M')
                except ValueError:
                    return datetime.strptime(date_time_string, '%m/%d/%Y %H:%M')
