from datetime import datetime


class NoYearDatetimeFormatter:
    DATE_TIME_FORMAT = '%m-%d %H:%M:%S.%f'

    def format(self, date_time):
        return date_time.strftime(self.DATE_TIME_FORMAT)

    def get_source_type(self):
        return datetime
