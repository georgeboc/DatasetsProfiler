NULLABLE = True


class ParserCommons:
    def nullify_missing_fields(self, fields: tuple):
        return tuple(field if field else None for field in fields)

    def split_by_comma_outside_quotes(self, string):
        if not string:
            return []
        current_section_start = 0
        current_section_end = 0
        is_within_quotes = False
        sections = []
        for position, character in enumerate(string):
            current_section_end = position
            if character == '"':
                is_within_quotes = not is_within_quotes
            elif character == ',' and not is_within_quotes:
                sections.append(string[current_section_start:current_section_end])
                current_section_start = position + 1
        sections.append(string[current_section_start:current_section_end + 1])
        return sections
