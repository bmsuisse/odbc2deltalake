from typing import Union
import sqlglot.expressions as ex

table_name_type = Union[str, tuple[str, str]]


def is_string_type(type: Union[ex.DataType, ex.DataType.Type]):
    if isinstance(type, ex.DataType):
        return type.is_string or type.this in ex.DataType.TEXT_TYPES
    return type in ex.DataType.TEXT_TYPES
