from dataclasses import dataclass


@dataclass
class ProcessorsOperationsFlags:
    column_statistics_calculate_average_count_is_enabled:                   bool = True
    column_statistics_calculate_entropy_is_enabled:                         bool = True
    column_statistics_calculate_min_max_is_enabled:                         bool = True
    column_statistics_calculate_variance_is_enabled:                        bool = True
    column_statistics_calculate_standard_deviation_is_enabled:              bool = True

    numeric_processor_calculate_distinct_is_enabled:                        bool = True
    numeric_processor_calculate_not_null_rows_count_is_enabled:             bool = True
    numeric_processor_calculate_null_rows_count_is_enabled:                 bool = True

    string_processor_calculate_characters_entropy:                          bool = True
    string_processor_calculate_messages_entropy:                            bool = True
    string_processor_calculate_not_null_rows_count_is_enabled:              bool = True
    string_processor_calculate_null_rows_count_is_enabled:                  bool = True
    string_processor_calculate_words_entropy_is_enabled:                    bool = True
    string_processor_calculate_distinct_messages_count_is_enabled:          bool = True
    string_processor_calculate_distinct_words_count_is_enabled:             bool = True
    string_processor_calculate_distinct_characters_count_is_enabled:        bool = True
    string_processor_get_count_blank_spaces_statistics_is_enabled:          bool = True
    string_processor_get_message_length_statistics_is_enabled:              bool = True
    string_processor_get_words_length_statistics_is_enabled:                bool = False

    timestamp_processor_calculate_delta_time_in_seconds_entropy_is_enabled: bool = False
    timestamp_processor_calculate_distinct_rows_count_is_enabled:           bool = True
    timestamp_processor_calculate_not_null_rows_count_is_enabled:           bool = True
    timestamp_processor_calculate_null_rows_count_is_enabled:               bool = True
    timestamp_processor_calculate_timestamp_entropy_is_enabled:             bool = True
    timestamp_processor_get_delta_time_in_seconds_statistics_is_enabled:    bool = True

    tuple_processor_calculate_tuple_entropy_is_enabled:                     bool = True
