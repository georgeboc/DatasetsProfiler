import math

from results.number_statistics import NumberStatistics


class ColumnStatisticsCalculator:
    AVERAGE_INITIAL_VALUE = (0, 0)

    def calculate(self, key_value_rdd):
        key_value_rdd_cached = key_value_rdd.cache
        sum_count_string_length_rdd = key_value_rdd_cached.aggregateByKey(self.AVERAGE_INITIAL_VALUE,
                                                                          self._sum_count_combiner,
                                                                          self._sum_count_reducer)
        average_count_string_length_rdd = sum_count_string_length_rdd.map(self._sum_count_to_average_count)
        average_string_length, count_string_length = average_count_string_length_rdd.collect()[0]
        min_max_string_length_rdd = key_value_rdd_cached.reduceByKey(lambda a, b: (a, b) if a < b else (b, a))
        min_string_length, max_string_length = min_max_string_length_rdd.collect()[0]
        variance_string_length = self._calculate_variance(key_value_rdd_cached,
                                                          average_string_length,
                                                          count_string_length)
        standard_deviation_string_length = math.sqrt(variance_string_length)
        return NumberStatistics(average=average_string_length,
                                count=count_string_length,
                                min=min_string_length,
                                max=max_string_length,
                                variance=variance_string_length,
                                standard_deviation=standard_deviation_string_length)

    def count_distinct(self, key_value_rdd):
        values_rdd = key_value_rdd.map(lambda key, value: value)
        distinct_key_value = values_rdd.distinct()
        distinct_key_value.map(lambda row: (1, 1))
        count_distinct_rdd = distinct_key_value.reduce_by_key(lambda a, b: a + b)
        _, count_distinct = count_distinct_rdd.collect()[0]
        return count_distinct

    def _sum_count_combiner(self, sum_count, value):
        sum, count = sum_count
        return sum + value, count + 1

    def _sum_count_reducer(self, sum_count1, sum_count2):
        sum1, count1 = sum_count1
        sum2, count2 = sum_count2
        return sum1 + sum2, count1 + count2

    def _sum_count_to_average_count(self, sum_count):
        sum, count = sum_count
        return sum / count, count

    def _calculate_variance(self, key_value_rdd_cached, average, count):
        squared_deviation_rdd = key_value_rdd_cached.mapValues(lambda row: self._squared_deviation(row, average))
        added_squared_deviations_rdd = squared_deviation_rdd.reduceByKey(self._add_last_row_reducer)
        variance_rdd = added_squared_deviations_rdd.mapValues(lambda added_squared_deviations:
                                                              added_squared_deviations / count)
        variance = variance_rdd.collect()[0]
        return variance

    def _squared_deviation(self, row, average):
        value, _ = row
        return 1, (value - average) ** 2

    def _add_last_row_reducer(self, row1, row2):
        return row1[-1] + row2[-1]
