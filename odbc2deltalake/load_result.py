from typing import Literal, Union, Any, Optional


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

    dirty: Optional[bool] = None  # True if the count does not match

    def __init__(self):
        self.executed_type = "delta"


class NoLoadResult:
    executed_type: Literal["none"]

    def __init__(self):
        self.executed_type = "none"


class AppendOnlyLoadResult:
    executed_type: Literal["append_only"]

    def __init__(self):
        self.executed_type = "append_only"


LoadResult = Union[FullLoadResult, DeltaLoadResult, NoLoadResult, AppendOnlyLoadResult]
