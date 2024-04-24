from typing import Optional, Union, Any
from .query import sql_quote_value, sql_quote_name
from odbc2deltalake.metadata import FieldWithType
import sqlglot.expressions as ex

table_name_type = Union[str, tuple[str, str]]


def is_string_type(type: ex.DataType | ex.DataType.Type):
    if isinstance(type, ex.DataType):
        return type.is_string or type.this in ex.DataType.TEXT_TYPES
    return type in ex.DataType.TEXT_TYPES
