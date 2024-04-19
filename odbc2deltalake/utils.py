from typing import TypeVar, Sequence

T = TypeVar("T")


def concat_seq(*args: Sequence[T]) -> Sequence[T]:
    if len(args) == 1:
        return args[0]
    r = list()
    for a in args:
        r = r + a  # type: ignore

    return r
