from datetime import datetime
from pyspark.sql.types import StringType, TimestampType, FloatType

from parsers.parser_commons import NULLABLE
from parsers.parser_strategy import ParserStrategy


class EdgarLogParserStrategy(ParserStrategy):
    def __init__(self, parser_commons):
        self._parser_commons = parser_commons

    def parse(self, row):
        row_string = row[0]
        ip_s, date_s, time_s, zone_s, cik_s, accession_s, extention_s, code_s, size_s, idx_s, norefer_s, \
        noagent_s, find_s, crawler_s, browser_s = self._parser_commons.nullify_missing_fields(row_string.split(','))
        date_time = datetime.strptime(f"{date_s} {time_s}", '%Y-%m-%d %H:%M:%S') if date_s and time_s else None
        zone = float(zone_s) if zone_s else None
        cik = float(cik_s) if cik_s else None
        code = float(code_s) if code_s else None
        size = float(size_s) if size_s else None
        idx = float(idx_s) if idx_s else None
        norefer = float(norefer_s) if norefer_s else None
        noagent = float(noagent_s) if noagent_s else None
        find = float(find_s) if find_s else None
        crawler = float(crawler_s) if crawler_s else None
        browser = float(browser_s) if browser_s else None
        return ip_s, date_time, zone, cik, accession_s, extention_s, code, size, idx, norefer, noagent, find, \
               crawler, browser

    def get_schema(self):
        return [
            ("IP", StringType(), NULLABLE),
            ("DateTime", TimestampType(), NULLABLE),
            ("Zone", FloatType(), NULLABLE),
            ("CIK", FloatType(), NULLABLE),
            ("Accession", StringType(), NULLABLE),
            ("Extention", StringType(), NULLABLE),
            ("Code", FloatType(), NULLABLE),
            ("Size", FloatType(), NULLABLE),
            ("IDX", FloatType(), NULLABLE),
            ("NoRefer", FloatType(), NULLABLE),
            ("NoAgent", FloatType(), NULLABLE),
            ("Find", FloatType(), NULLABLE),
            ("Crawler", FloatType(), NULLABLE),
            ("Browser", StringType(), NULLABLE)
        ]

    def is_header_present(self):
        return True
