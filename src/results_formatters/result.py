from dataclasses import dataclass


@dataclass
class Result:
    type: str = None
    dictionary: dict = None
