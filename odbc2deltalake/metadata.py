from typing import Callable, Literal, Union, Optional, Any, TYPE_CHECKING
import logging

from odbc2deltalake.query import sql_quote_name
from .reader import DataSourceReader
from pydantic import BaseModel
import sqlglot
import sqlglot.expressions as ex

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

    query = sqlglot.parse_one(
        f"""SELECT ccu.COLUMN_NAME
    FROM {quoted_db}INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc WITH(NOLOCK)
        JOIN {quoted_db}INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu WITH(NOLOCK) ON tc.CONSTRAINT_NAME = ccu.Constraint_name
    WHERE tc.CONSTRAINT_TYPE = 'Primary Key'""",
        dialect=dialect,
    )
    assert isinstance(query, ex.Select)
    query = query.where(
        ex.column("TABLE_NAME", "ccu")
        .eq(ex.convert(real_table_name))
        .and_(ex.column("TABLE_SCHEMA", "ccu").eq(ex.convert(real_schema)))
    )
    full_query = query.sql(dialect)
    return [d["COLUMN_NAME"] for d in reader.source_sql_to_py(full_query)]


class FieldWithType(BaseModel):
    name: str
    type: str
    max_str_length: int | None = None


class InformationSchemaColInfo(BaseModel):
    column_name: str

    @property
    def compat_name(self):
        invalid_chars = " ,;{}()\n\t="
        res = self.column_name
        for ic in invalid_chars:
            res = res.replace(ic, "_")
        return res

    data_type: str
    column_default: str | None = None
    is_nullable: bool = True
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    datetime_precision: int | None = None
    generated_always_type_desc: Literal[
        "NOT_APPLICABLE", "AS_ROW_START", "AS_ROW_END"
    ] = "NOT_APPLICABLE"

    def as_field_type(self, *, compat: bool):

        return FieldWithType(
            name=self.column_name if not compat else self.compat_name,
            type=self.data_type,
            max_str_length=self.character_maximum_length,
        )

    @staticmethod
    def from_name_type(name: str, type: str) -> "InformationSchemaColInfo":
        if "(" in type:
            type, rest = type.split("(", 1)
            rest = rest[:-1]
            if "," in rest:
                precision, scale = rest.split(",", 1)
                precision = int(precision)
                scale = int(scale)
            else:
                precision = int(rest)
                scale = None
        else:
            precision = None
            scale = None
        return InformationSchemaColInfo(
            column_name=name,
            column_default=None,
            is_nullable=True,
            data_type=type,
            character_maximum_length=(
                precision if type in ["varchar", "char", "nvarchar", "nchar"] else None
            ),
            numeric_precision=precision if type in ["decimal", "numeric"] else None,
            numeric_scale=scale if type in ["decimal", "numeric"] else None,
            datetime_precision=None,
            generated_always_type_desc="NOT_APPLICABLE",
        )


def get_columns(
    reader: DataSourceReader, table_name: table_name_type, *, dialect: str
) -> list[InformationSchemaColInfo]:
    if isinstance(table_name, str):
        table_name = ("dbo", table_name)
    real_table_name = table_name[1] if len(table_name) == 2 else table_name[2]
    real_schema = table_name[0] if len(table_name) == 2 else table_name[1]
    real_db = table_name[0] if len(table_name) == 3 else None
    quoted_db = sql_quote_name(real_db) + "." if real_db else ""

    query = sqlglot.parse_one(
        f""" SELECT  ccu.column_name, ccu.column_default,
		cast(case when ccu.IS_NULLABLE='YES' THEN 1 ELSE 0 END as bit) as is_nullable,
		data_type,
		character_maximum_length,
		numeric_precision,
		numeric_scale,
		datetime_precision,
        ci.generated_always_type_desc FROM {quoted_db}INFORMATION_SCHEMA.COLUMNS ccu
        left join (
			
SELECT sc.name as schema_name, t.name as table_name, c.name as col_name, c.generated_always_type_desc FROM {quoted_db}sys.columns c 
	inner join {quoted_db}sys.tables t on t.object_id=c.object_id
	inner join {quoted_db}sys.schemas sc on sc.schema_id=t.schema_id
		) ci on ci.schema_name=ccu.TABLE_SCHEMA and ci.table_name=ccu.TABLE_NAME and ci.col_name=ccu.COLUMN_NAME
		 """,
        dialect=dialect,
    )
    assert isinstance(query, ex.Select)
    full_query = query.where(
        ex.column("TABLE_NAME", "ccu")
        .eq(ex.convert(real_table_name))
        .and_(ex.column("TABLE_SCHEMA", "ccu").eq(ex.convert(real_schema)))
    ).sql(dialect)
    dicts = reader.source_sql_to_py(full_query)
    return [InformationSchemaColInfo(**d) for d in dicts]


def get_compatibility_level(reader: DataSourceReader) -> int:
    return reader.source_sql_to_py(
        "SELECT compatibility_level FROM sys.databases WHERE name =DB_NAME()"
    )[0]["compatibility_level"]
