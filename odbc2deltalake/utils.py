from typing import TypeVar, Sequence

T = TypeVar("T")


def concat_seq(*args: Sequence[T]) -> Sequence[T]:
    if len(args) == 1:
        return args[0]
    r = list()
    for a in args:
        r = r + a  # type: ignore

    return r


def _is_pydantic_2() -> bool:
    import pydantic

    try:
        # __version__ was added with Pydantic 2 so we know if this errors the version is < 2.
        # Still check the version as a fail safe incase __version__ gets added to verion 1.
        if int(pydantic.__version__[:1]) >= 2:  # type: ignore[attr-defined]
            return True

        # Raise an AttributeError to match the AttributeError on __version__ because in either
        # case we need to get to the same place.
        raise AttributeError  # pragma: no cover
    except AttributeError:  # pragma: no cover
        return False


is_pydantic_2 = _is_pydantic_2()
