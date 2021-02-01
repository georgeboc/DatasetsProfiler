NULLABLE = True


class ParserCommons:
    def nullify_missing_fields(self, fields: tuple):
        return tuple(field if field else None for field in fields)
