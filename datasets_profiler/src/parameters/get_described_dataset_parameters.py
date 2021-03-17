from dataclasses import dataclass
from typing import Optional, List


@dataclass
class GetDescribedDatasetParameters:
    use_case: str
    output_directory: str
    input_path: str = "Datasets/Test/Test.log"
    parser: str = "test_log_parser_strategy"
    limit: Optional[int] = None
