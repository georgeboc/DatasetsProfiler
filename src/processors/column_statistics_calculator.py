import math

from results.number_statistics import NumberStatistics


class ColumnStatisticsCalculator:
    ZERO_PAIR_INITIAL_VALUE = (0, 0)

    def calculate_number_statistics(self, key_value_rdd):
        key_value_rdd_cached = key_value_rdd.cache()
        average, count = self.calculate_average_count(key_value_rdd_cached)
        min, max = self.calculate_min_max(key_value_rdd_cached)
        variance = self.calculate_variance(key_value_rdd_cached, average, count)
        standard_deviation = math.sqrt(variance)
        return NumberStatistics(average=average,
                                count=count,
                                min=min,
                                max=max,
                                variance=variance,
                                standard_deviation=standard_deviation)

    def calculate_min_max(self, key_value_rdd_cached):
        min_max_rdd = key_value_rdd_cached.aggregateByKey(self.ZERO_PAIR_INITIAL_VALUE,
                                                          self._min_max_combiner,
                                                          self._min_max_reducer)
        _, min_max = min_max_rdd.first()
        return min_max

    def calculate_average_count(self, key_value_rdd_cached):
        sum_count_rdd = key_value_rdd_cached.aggregateByKey(self.ZERO_PAIR_INITIAL_VALUE,
                                                            self._sum_count_combiner,
                                                            self._sum_count_reducer)
        average_count_rdd = sum_count_rdd.mapValues(self._sum_count_to_average_count)
        _, average_count = average_count_rdd.first()
        return average_count

    def calculate_variance(self, key_value_rdd_cached, average, count):
        squared_deviation_rdd = key_value_rdd_cached.mapValues(lambda value: self._squared_deviation(value, average))
        added_squared_deviations_rdd = squared_deviation_rdd.reduceByKey(lambda a, b: a + b)
        variance_rdd = added_squared_deviations_rdd.mapValues(lambda added_squared_deviations:
                                                              added_squared_deviations / count)
        _, variance = variance_rdd.first()
        return variance

    def calculate_entropy(self, key_value_rdd_cached):
        frequencies = key_value_rdd_cached.countByValue().values()
        rows_count = key_value_rdd_cached.count()
        probabilites = map(lambda frequency: frequency/rows_count, frequencies)
        ponderated_information_quantity = map(lambda probability: probability*math.log2(1/probability), probabilites)
        return sum(ponderated_information_quantity)

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

    def _min_max_combiner(self, min_max, value):
        if min_max == self.ZERO_PAIR_INITIAL_VALUE:
            return (value, value)
        min, max = min_max
        if value < min:
            min = value
        if value > max:
            max = value
        return min, max

    def _min_max_reducer(self, min_max1, min_max2):
        min1, max1 = min_max1
        min2, max2 = min_max2
        min = min1
        max = max2
        if min1 > min2:
            min = min2
        if max1 > max2:
            max = max2
        return min, max

    def _squared_deviation(self, value, average):
        return (value - average) ** 2
