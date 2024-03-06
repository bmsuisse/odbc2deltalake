
from pathlib import Path
from typing import Sequence, Any, TypeVar
import sqlglot.expressions as ex


def union(selects: Sequence[ex.Expression], *, distinct: bool) -> ex.Expression:
    if len(selects) == 0:
        raise ValueError("No selects to union")
    elif len(selects) == 1:
        return selects[0]
    elif len(selects) == 2:
        return ex.union(selects[0], selects[1], distinct=distinct)
    else:
        return ex.union(selects[0], union(selects[1:], distinct=distinct), distinct=distinct)

def table_from_tuple(name: str | tuple[str, str] | tuple[str, str, str], alias: str | None = None) -> ex.Table:
    if alias is not None:
        assert " " not in alias
        assert "-" not in alias
        assert "'" not in alias
        assert '"' not in alias
        assert "*" not in alias
        assert "/" not in alias
        assert "\\" not in alias

    if isinstance(name, str):
        return ex.Table(
            this=ex.Identifier(this=name, quoted=True),
            alias=ex.Identifier(this=alias, quoted=False) if alias else None,
        )
    if len(name) == 2:
        return ex.Table(
            this=ex.Identifier(this=name[1], quoted=True),
            db=ex.Identifier(this=name[0], quoted=True),
            alias=ex.Identifier(this=alias, quoted=False) if alias else None,
        )
    if len(name) == 3:
        return ex.Table(
            this=ex.Identifier(this=name[2], quoted=True),
            db=ex.Identifier(this=name[1], quoted=True),
            catalog=ex.Identifier(this=name[0], quoted=True),
            alias=ex.Identifier(this=alias, quoted=False) if alias else None,
        )
    raise ValueError(f"Invalid name: {name}")
