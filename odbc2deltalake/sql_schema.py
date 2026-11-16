from typing import Union

import sqlglot.expressions as ex

table_name_type = Union[str, tuple[str, str]]


def is_string_type(data_type: Union[ex.DataType, ex.DataType.Type]):
    if isinstance(data_type, ex.DataType):
        return data_type.is_string or data_type.this in ex.DataType.TEXT_TYPES
    return data_type in ex.DataType.TEXT_TYPES
