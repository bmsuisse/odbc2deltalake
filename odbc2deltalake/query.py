
from typing import Any, Literal, Tuple, TypeAlias, Union, overload
from datetime import datetime


SQLObjectNameType: TypeAlias = Union[str, Tuple[str, str], Tuple[str, str, str]]

QuoteMode: TypeAlias = Literal["ansi", "tsql", "postgres"]

def sql_quote_name(inp: SQLObjectNameType, *, mode: QuoteMode = "ansi", compat: bool = False):
    if compat:
        inp = get_compatible_name(inp)
    if isinstance(inp, str):
        if inp.startswith("#"):  # temp tables names must not be quoted
            assert " " not in inp
            assert "-" not in inp
            assert "'" not in inp
            assert '"' not in inp
            assert "*" not in inp
            assert "/" not in inp
            assert "\\" not in inp
            return inp
        assert "[" not in inp
        assert "]" not in inp
        assert "`" not in inp
        assert '"' not in inp
        if mode == "ansi":
            return '"' + inp + '"'
        if mode == "postgres":
            return "`" + inp + "`"
        return "[" + inp + "]"
    elif len(inp) == 3:
        db = sql_quote_name(inp[0], mode=mode)
        schema_name = sql_quote_name(inp[1], mode=mode)
        tbl_name = sql_quote_name(inp[2], mode=mode)
        return db + "." + schema_name + "." + tbl_name
    else:
        schema_name = sql_quote_name(inp[0], mode=mode)
        tbl_name = sql_quote_name(inp[1], mode=mode)
        return schema_name + "." + tbl_name


def sql_quote_value(vl: Any):
    if vl is None:
        return "null"
    if isinstance(vl, str):
        return "'" + vl.replace("'", "''") + "'"
    if isinstance(vl, float):
        return str(vl)
    if isinstance(vl, int):
        return str(vl)
    if isinstance(vl, bool):
        return "1" if vl else "0"
    if isinstance(vl, datetime):
        return "'" + vl.isoformat() + "'"
    return "'" + str(vl).replace("'", "''") + "'"

@overload
def get_compatible_name(name: str) -> str:
    ...


@overload
def get_compatible_name(name: tuple[str, str]) -> tuple[str, str]:
    ...


@overload
def get_compatible_name(name: tuple[str, str, str]) -> tuple[str, str, str]:
    ...


def get_compatible_name(name: SQLObjectNameType) -> SQLObjectNameType:
    """This removes spaces and some special characters from the name and replaces them with underscores or similar chars.

    Args:
        name (SQLObjectNameType): The name to make compatible

    Returns:
        SQLObjectNameType: A compatible name
    """
    if isinstance(name, str):
        first_char = name[0]
        if not (
            (ord(first_char) >= ord("a") and ord(first_char) <= ord("z"))
            or (ord(first_char) >= ord("A") and ord(first_char) <= ord("Z"))
        ):
            return get_compatible_name("c_" + name)
        replace_map = {"é": "e", "ê": "e", "â": "a", "è": "e", "ä": "a", "ö": "o", "ü": "u", " ": "_"}
        for tr, tv in replace_map.items():
            name = name.replace(tr, tv)
        return name
    else:
        assert isinstance(name, tuple)
        return tuple((get_compatible_name(n) for n in name))  # type: ignore
