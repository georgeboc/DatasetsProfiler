import re
from statistics import mean, median, stdev

FIRST = 1
CSV_HEADERS = "Function name;Execution time;;;;;Call count\n;Median in seconds;Mean in seconds;Min in seconds;Max in " \
              "seconds;Standard deviation in seconds "
SEPARATOR = ';'


def stringify(number):
    return str(number).replace('.', ',')


def main():
    is_first_line = True

    print("Insert line to be processed: ")
    while line := input():
        tab_pattern = r'	 '
        function_name = line.split('	')[0]
        with_comma = re.sub(tab_pattern, ",", line)
        execution_times_in_seconds = []
        delta_time_seconds_and_microseconds_pattern = r'\(seconds=(\d+),microseconds=(\d+)\)'
        if seconds_and_microseconds := re.findall(delta_time_seconds_and_microseconds_pattern, with_comma):
            execution_times_in_seconds.extend([float(seconds) + float(microseconds)*10**(-6)
                                               for seconds, microseconds in seconds_and_microseconds])
        delta_time_microseconds_pattern = r'\(microseconds=(\d+)\)'
        if microseconds := re.findall(delta_time_microseconds_pattern, with_comma):
            execution_times_in_seconds.extend([float(microseconds) * 10 ** (-6)
                                               for microseconds in microseconds])
        # print(execution_times_in_seconds)

        call_count_pattern = r'call_count=(\d+)'
        call_count = int(re.search(call_count_pattern, with_comma).group(FIRST))

        stats = [function_name,
                 stringify(median(execution_times_in_seconds)),
                 stringify(mean(execution_times_in_seconds)),
                 stringify(min(execution_times_in_seconds)),
                 stringify(max(execution_times_in_seconds)),
                 stringify(stdev(execution_times_in_seconds)) if len(execution_times_in_seconds) > 1 else '-',
                 stringify(call_count)]

        if is_first_line:
            print(CSV_HEADERS)
            is_first_line = False
        print(SEPARATOR.join(stats))


if __name__ == "__main__":
    main()