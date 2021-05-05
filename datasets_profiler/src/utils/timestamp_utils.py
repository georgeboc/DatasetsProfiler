from datetime import datetime


class TimestampUtils:
    COLON = ':'
    SPACE = ' '
    HYPHEN = '-'
    UNDERSCORE = '_'

    @staticmethod
    def get_file_name_from_now():
        timestamp = datetime.now().isoformat()
        return timestamp \
            .replace(TimestampUtils.COLON, TimestampUtils.HYPHEN) \
            .replace(TimestampUtils.SPACE, TimestampUtils.UNDERSCORE)
