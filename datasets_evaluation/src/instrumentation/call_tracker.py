from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List


@dataclass
class CallStatistics:
    execution_times: List[timedelta]
    call_count: int


class StatefulCallTracker:
    def __init__(self):
        self._call_trackers = {}

    def register_call(self, function_name, execution_time):
        call_statistics = self._call_trackers.setdefault(function_name,
                                                         CallStatistics(execution_times=[], call_count=0))
        call_statistics.call_count += 1
        call_statistics.execution_times.append(execution_time)

    def get_call_trackers_dictionary(self):
        return self._call_trackers

    def clear_state(self):
        self._call_trackers.clear()


def instrument_call(function):
    def _get_key(class_name, function_name):
        return f"{class_name}.{function_name}"

    def wrapper(self, *args, **kwargs):
        start_time = datetime.now()
        result = function(self, *args, **kwargs)
        end_time = datetime.now()
        execution_time = end_time - start_time
        self._call_tracker.register_call(_get_key(self.__class__.__name__, function.__name__), execution_time)
        return result
    return wrapper
