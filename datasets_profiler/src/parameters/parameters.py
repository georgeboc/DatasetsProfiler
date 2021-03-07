from dataclasses import dataclass
from typing import Optional, List


@dataclass
class Parameters:
    output_path: str
    input_path: str = "Datasets/Test/Test.log"
    parser: str = "test_log_parser_strategy"
    formatters: Optional[List[str]] = None
    limit: Optional[int] = None
