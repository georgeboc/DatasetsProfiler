from dataclasses import dataclass
from typing import Any


@dataclass
class ApplicationInitialization:
    spark_configuration: Any
    reader: Any
    row_dispatcher: Any
    tuple_processor: Any
    column_statistics_calculator: Any
    results_viewer: Any
    interface: Any
    parser_providers: Any
