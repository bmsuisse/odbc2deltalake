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


def sql_quote_value_with_type(
    type_str: str, max_str_length: int | None, value: Any
) -> str:
    if value is not None and type_str.lower() in ["bit", "bool", "boolean"]:
        assert type(value) == bool
        if value == True:
            return f"CAST(1 as bit)"
        else:
            return f"CAST(0 as bit)"
    if value is not None and type_str.lower() in ["string", "int32", "varchar", "int"]:
        return sql_quote_value(value)  # string / int32 are defaults for sql server
    sql_type = get_sql_type(type_str, max_str_length)
    if value is not None:
        return f"CAST({sql_quote_value(value)} as {sql_type})"
    else:
        return f"CAST(NULL AS {sql_type})"


def _get_col_definition(
    field: FieldWithType,
    nullable: bool,
) -> str:
    sql_type = get_sql_type(field.type, field.max_str_length)
    definit = (
        sql_quote_name(field.name)
        + " "
        + sql_type
        + (" NOT NULL" if not nullable else "")
    )
    return definit


def get_sql_for_schema(
    table_name: table_name_type,
    schema: list[FieldWithType],
    primary_keys: list[str] | None,
    with_exist_check: bool,
):
    cols_sql = [
        _get_col_definition(
            f,
            primary_keys is None or f.name not in primary_keys,
        )
        for f in schema
    ]

    cols = ", ".join(cols_sql)
    pkdef = ""
    if primary_keys and len(primary_keys) > 0:
        pkcols = ", ".join((sql_quote_name(n) for n in primary_keys))
        tbl_name_pk = (
            table_name.removeprefix("##")
            if isinstance(table_name, str)
            else table_name[0] + "_" + table_name[1]
        )
        pkdef = (
            f", CONSTRAINT {sql_quote_name('PK_'+tbl_name_pk)}  PRIMARY KEY({pkcols})"
        )
    create_sql = f"CREATE TABLE {sql_quote_name(table_name)}({cols}{pkdef}) "
    if with_exist_check:
        return f"""
            IF OBJECT_ID (N'{sql_quote_name(table_name).replace("'", "''")}', N'U') IS NULL 
            BEGIN
                {create_sql}
                select 'created' as action
            END
            else
            begin
                select 'nothing' as action
            end 
        """
    return create_sql
