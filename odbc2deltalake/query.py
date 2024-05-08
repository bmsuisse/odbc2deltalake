from typing import Any, Literal, Tuple, Union
from typing_extensions import TypeAlias
from datetime import datetime


SQLObjectNameType: TypeAlias = Union[str, Tuple[str, str], Tuple[str, str, str]]

QuoteMode: TypeAlias = Literal["ansi", "tsql", "postgres"]


def sql_quote_name(inp: SQLObjectNameType, *, mode: QuoteMode = "ansi"):
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
