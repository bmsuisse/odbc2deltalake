from typing import Literal, Union

from odbc2deltalake.query import sql_quote_name
from .reader import DataSourceReader
from pydantic import BaseModel
import sqlglot
import sqlglot.expressions as ex
from .utils import is_pydantic_2

if is_pydantic_2:
    from pydantic import ConfigDict

table_name_type = Union[str, tuple[str, str], tuple[str, str, str]]


def get_primary_keys(
    reader: DataSourceReader, table_name: table_name_type, *, dialect: str
) -> list[str]:
    if isinstance(table_name, str):
        table_name = ("dbo", table_name)
    real_table_name = table_name[1] if len(table_name) == 2 else table_name[2]
    real_schema = table_name[0] if len(table_name) == 2 else table_name[1]
    real_db = table_name[0] if len(table_name) == 3 else None
    quoted_db = sql_quote_name(real_db) + "." if real_db else ""
    no_lock = "WITH(NOLOCK)" if dialect == "mssql" else ""
    query = sqlglot.parse_one(
        f"""SELECT ccu.COLUMN_NAME as col_name
    FROM {quoted_db}INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc {no_lock}
        JOIN {quoted_db}INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu {no_lock} ON tc.CONSTRAINT_NAME = ccu.Constraint_name
    WHERE lower(tc.CONSTRAINT_TYPE) = 'primary key'""",
        dialect=dialect,
    )
    assert isinstance(query, ex.Select)
    query = query.where(
        ex.func("lower", ex.column("TABLE_NAME", "ccu"))
        .eq(ex.convert(real_table_name.lower()))
        .and_(
            ex.func("lower", ex.column("TABLE_SCHEMA", "ccu")).eq(
                ex.convert(real_schema.lower())
            )
        )
    )
    full_query = query.sql(dialect)
    res = reader.source_sql_to_py(full_query)
    return [d["col_name"] for d in res]


class FieldWithType(BaseModel):
    name: str
    type: str
    max_str_length: Union[int, None] = None


class InformationSchemaColInfo(BaseModel):
    if is_pydantic_2:
        model_config = ConfigDict(arbitrary_types_allowed=True)
    if not is_pydantic_2:

        class Config:
            arbitrary_types_allowed = True

    column_name: str

    data_type: ex.DataType
    data_type_str: str
    column_default: Union[str, None] = None
    is_nullable: bool = True
    generated_always_type_desc: Union[
        Literal["NOT_APPLICABLE", "AS_ROW_START", "AS_ROW_END"], None
    ] = "NOT_APPLICABLE"
    is_identity: bool = False


def _get_table_cols(
    reader: DataSourceReader, table_name: table_name_type, *, dialect: str
):
    if isinstance(table_name, str):
        table_name = ("dbo", table_name)
    real_table_name = table_name[1] if len(table_name) == 2 else table_name[2]
    real_schema = table_name[0] if len(table_name) == 2 else table_name[1]
    real_db = table_name[0] if len(table_name) == 3 else None
    quoted_db = sql_quote_name(real_db) + "." if real_db else ""
    if dialect not in ["tsql", "mssql"]:
        query = sqlglot.parse_one(
            f""" 
            SELECT  ccu.column_name, ccu.column_default,
            ccu.IS_NULLABLE=='YES' as is_nullable,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            null as generated_always_type_desc,
            ccu.is_identity=='yes' as is_identity FROM {quoted_db}INFORMATION_SCHEMA.COLUMNS ccu
            """,
            dialect=dialect,
        )
    else:
        query = sqlglot.parse_one(
            f""" SELECT  ccu.column_name, ccu.column_default,
            cast(case when ccu.IS_NULLABLE='YES' THEN 1 ELSE 0 END as bit) as is_nullable,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            ci.generated_always_type_desc,
            coalesce(ci.is_identity, convert(bit, 0)) as is_identity FROM {quoted_db}INFORMATION_SCHEMA.COLUMNS ccu
            left join (
                
    SELECT sc.name as table_schema, t.name as table_name, c.name as col_name, c.generated_always_type_desc, c.is_identity as is_identity FROM {quoted_db}sys.columns c 
        inner join {quoted_db}sys.tables t on t.object_id=c.object_id
        inner join {quoted_db}sys.schemas sc on sc.schema_id=t.schema_id
            ) ci on ci.table_schema=ccu.TABLE_SCHEMA and ci.table_name=ccu.TABLE_NAME and ci.col_name=ccu.COLUMN_NAME
            """,
            dialect=dialect,
        )
    assert isinstance(query, ex.Select)
    full_query = query.where(
        ex.func("LOWER", ex.column("TABLE_NAME", "ccu"))
        .eq(ex.func("LOWER", ex.convert(real_table_name)))
        .and_(
            ex.func("LOWER", ex.column("TABLE_SCHEMA", "ccu")).eq(
                ex.func("LOWER", ex.convert(real_schema))
            )
        )
    ).sql(dialect)
    dicts = reader.source_sql_to_py(full_query)
    for d in dicts:
        dt = d.pop("data_type")
        max_len = d.pop("character_maximum_length")
        numeric_precision = d.pop("numeric_precision")
        numeric_scale = d.pop("numeric_scale")
        datetime_precision = d.pop("datetime_precision")
        if dt in ["xml"]:
            max_len = None
        if dt.lower() not in ["numeric", "decimal"]:
            numeric_precision = None
            numeric_scale = None
        dt_str = dt
        if max_len is not None:
            dt_str += f"({max_len})" if max_len != -1 else "(MAX)"
        elif numeric_precision is not None:
            dt_str += f"({numeric_precision},{numeric_scale})"
        elif datetime_precision and dt.lower() not in ["datetime", "date"]:
            dt_str += f"({datetime_precision})"

        d["data_type"] = ex.DataType.build(
            dt_str, dialect=dialect if dt_str != "xml" else "tsql"
        )  # Hack to resolve https://github.com/tobymao/sqlglot/issues/5393
        d["data_type_str"] = dt_str
    return [InformationSchemaColInfo(**d) for d in dicts]


def _get_query_cols_first_result_set(
    reader: DataSourceReader, query: ex.Query, *, dialect: str
):
    sql = (
        "EXEC sp_describe_first_result_set @tsql=N'"
        + query.sql(dialect).replace("'", "''")
        + "'"
    )

    dicts = reader.source_sql_to_py(sql)

    for d in dicts:
        sys_type_name = d["system_type_name"]

        yield InformationSchemaColInfo(
            column_name=d["name"],
            data_type_str=sys_type_name,
            data_type=ex.DataType.build(sys_type_name, dialect=dialect),
            column_default=None,
            is_identity=d["is_identity_column"],
            generated_always_type_desc="NOT_APPLICABLE",
            is_nullable=d["is_nullable"],
        )


def get_columns(
    reader: DataSourceReader,
    table_or_query: Union[table_name_type, ex.Query],
    *,
    dialect: str,
) -> list[InformationSchemaColInfo]:
    if isinstance(table_or_query, ex.Query):
        if not reader.supports_proc_exec or dialect != "tsql":
            return reader.source_schema_limit_one(
                table_or_query
            )  # get result metadata by just executing the query with limit 0
        else:
            return list(
                _get_query_cols_first_result_set(
                    reader, table_or_query, dialect=dialect
                )
            )

    else:
        return _get_table_cols(reader, table_or_query, dialect=dialect)


def get_compatibility_level(reader: DataSourceReader) -> int:
    return reader.source_sql_to_py(
        "SELECT compatibility_level FROM sys.databases WHERE name =DB_NAME()"
    )[0]["compatibility_level"]
