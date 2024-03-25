from typing import Optional, Union, Any
from .query import sql_quote_value, sql_quote_name
from odbc2deltalake.metadata import FieldWithType

table_name_type = Union[str, tuple[str, str]]


def get_sql_type(type_str: str, max_str_length: int | None) -> str:
    if type_str in ["binary", "varbinary", "large_binary"]:
        real_max_length = max_str_length or 8000
        if real_max_length > 8000:
            real_max_length = "MAX"
        return f"varbinary({real_max_length})"
    if type_str in [
        "varchar",
        "nvarchar",
        "string",
        "large_string",
        "utf8",
        "large_utf8",
    ]:
        real_max_length = max_str_length or 4000
        if real_max_length <= 2000:
            real_max_length = real_max_length * 2
        elif real_max_length > 4000:  # need max type
            real_max_length = "MAX"
        return f"nvarchar({real_max_length})"
    return type_str
