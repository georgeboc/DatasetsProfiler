from dataclasses import dataclass
from typing import Optional, List


@dataclass
class GetDescriptionParameters:
    use_case: str
    output_path: str
    input_path: str = "Datasets/Test/Test.log"
    parser: str = "test_log_parser_strategy"
    formatter_names: Optional[List[str]] = None
    limit: Optional[int] = None
    disable_execution_metadata: bool = False
