from dataclasses import dataclass
from typing import Any


@dataclass
class ApplicationInitialization:
    spark_configuration: Any
    rdd_reader: Any
    column_dispatcher: Any
    tuple_processor: Any
    column_statistics_calculator: Any
    results_viewer: Any
    parser_providers: Any
    formatter_providers: Any
    results_formatter: Any
    checkpointer: Any
    interface_providers: Any
