from typing import Literal, Union, Any


class FullLoadResult:
    executed_type: Literal["full"]

    def __init__(self):
        self.executed_type = "full"


State = tuple[Any, int]


class DeltaLoadResult:
    executed_type: Literal["delta"]
    starting_local_state: State
    starting_source_state: State
    end_source_state: State

    dirty: bool | None = None  # True if the count does not match

    def __init__(self):
        self.executed_type = "delta"


class NoLoadResult:
    executed_type: Literal["none"]

    def __init__(self):
        self.executed_type = "none"


LoadResult = Union[FullLoadResult, DeltaLoadResult, NoLoadResult]
