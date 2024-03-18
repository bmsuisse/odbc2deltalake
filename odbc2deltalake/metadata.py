from typing import Callable, Literal, Union, Optional, Any, TYPE_CHECKING
import logging
from .reader import DataSourceReader
from pydantic import BaseModel
import sqlglot
import sqlglot.expressions as ex

table_name_type = Union[str, tuple[str, str]]


def get_primary_keys(
    reader: DataSourceReader, table_name: table_name_type
) -> list[str]:
    assert isinstance(table_name, tuple)
    query = sqlglot.parse_one(
        """SELECT ccu.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc WITH(NOLOCK)
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu WITH(NOLOCK) ON tc.CONSTRAINT_NAME = ccu.Constraint_name
    WHERE tc.CONSTRAINT_TYPE = 'Primary Key'""",
        dialect="tsql",
    )
    assert isinstance(query, ex.Select)
    query = query.where(
        ex.column("TABLE_NAME", "ccu")
        .eq(table_name[1])
        .and_(ex.column("TABLE_SCHEMA", "ccu").eq(table_name[0]))
    )
    full_query = query.sql("tsql")
    return [d["COLUMN_NAME"] for d in reader.source_sql_to_py(full_query)]


class FieldWithType(BaseModel):
    name: str
    type: str
    max_str_length: int | None = None


class InformationSchemaColInfo(BaseModel):
    column_name: str
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

    def as_field_type(self):

        return FieldWithType(
            name=self.column_name,
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
    reader: DataSourceReader, table_name: table_name_type
) -> list[InformationSchemaColInfo]:
    query = sqlglot.parse_one(
        """ SELECT  ccu.column_name, ccu.column_default,
		cast(case when ccu.IS_NULLABLE='YES' THEN 1 ELSE 0 END as bit) as is_nullable,
		data_type,
		character_maximum_length,
		numeric_precision,
		numeric_scale,
		datetime_precision,
        ci.generated_always_type_desc FROM INFORMATION_SCHEMA.COLUMNS ccu
        left join (
			
SELECT sc.name as schema_name, t.name as table_name, c.name as col_name, c.generated_always_type_desc FROM sys.columns c 
	inner join sys.tables t on t.object_id=c.object_id
	inner join sys.schemas sc on sc.schema_id=t.schema_id
		) ci on ci.schema_name=ccu.TABLE_SCHEMA and ci.table_name=ccu.TABLE_NAME and ci.col_name=ccu.COLUMN_NAME
		 """,
        dialect="tsql",
    )
    assert isinstance(query, ex.Select)
    full_query = query.where(
        ex.column("TABLE_NAME", "ccu")
        .eq(table_name[1])
        .and_(ex.column("TABLE_SCHEMA", "ccu").eq(table_name[0]))
    ).sql("tsql")
    dicts = reader.source_sql_to_py(full_query)
    return [InformationSchemaColInfo(**d) for d in dicts]


def get_compatibility_level(reader: DataSourceReader) -> int:
    return reader.source_sql_to_py(
        "SELECT compatibility_level FROM sys.databases WHERE name =DB_NAME()"
    )[0]["compatibility_level"]
