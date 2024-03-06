from dataclasses import dataclass
from typing import Callable, Literal, Union, Optional, Any, TYPE_CHECKING
import logging

from pydantic import BaseModel


table_name_type = Union[str, tuple[str, str]]

if TYPE_CHECKING:
    import pyodbc


def get_primary_keys(
    conn: "pyodbc.Connection", table_name: table_name_type | None
) -> dict[table_name_type, list[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """            
    exec sp_executesql N' SELECT ccu.TABLE_SCHEMA, ccu.TABLE_NAME, ccu.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc WITH(NOLOCK)
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu WITH(NOLOCK) ON tc.CONSTRAINT_NAME = ccu.Constraint_name
    WHERE tc.CONSTRAINT_TYPE = ''Primary Key'' AND (ccu.Table_Name=@Table or @Table is null) AND (ccu.TABLE_SCHEMA=@Schema or @Schema is null) ', N'@Table varchar(100), @Schema varchar(100)',
		@Schema=?, @Table=?
""",
            (
                table_name[0] if table_name else None,
                table_name[1] if table_name else None,
            ),
        )
        res = cur.fetchall()
        if res is None:
            return {}
        res_dict = {}
        for r in res:
            tn = r[0], r[1]
            if tn not in res_dict:
                res_dict[tn] = []
            res_dict[tn].append(r[2])
        if table_name and table_name not in res_dict:
            res_dict[table_name] = []
        return res_dict


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
    conn: "pyodbc.Connection", table_name: table_name_type | None
) -> dict[table_name_type, list[InformationSchemaColInfo]]:
    with conn.cursor() as cur:
        cur.execute(
            """            
     exec sp_executesql N' SELECT  ccu.TABLE_SCHEMA, ccu.TABLE_NAME,
		ccu.column_name, ccu.column_default,
		cast(case when ccu.IS_NULLABLE=''YES'' THEN 1 ELSE 0 end as bit) as is_nullable,
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
		where (ccu.Table_Name=@Table or @Table is null) AND (ccu.TABLE_SCHEMA=@Schema or @Schema is null) ', N'@Table varchar(100), @Schema varchar(100)',
		@Schema=?, @Table=?
""",
            (
                table_name[0] if table_name else None,
                table_name[1] if table_name else None,
            ),
        )
        res = cur.fetchall()
        if res is None:
            return {}
        res_dict = {}
        assert cur.description is not None
        col_names = [d[0] for d in cur.description]
        for r in res:
            r_dict = dict(zip(col_names, r))
            tn = r_dict.pop("TABLE_SCHEMA"), r_dict.pop("TABLE_NAME")
            if tn not in res_dict:
                res_dict[tn] = []
            res_dict[tn].append(InformationSchemaColInfo(**r_dict))
        return res_dict


def get_compatibility_level(connection: "pyodbc.Connection") -> int:
    with connection.cursor() as cur:
        cur.execute(
            """
    SELECT compatibility_level  
    FROM sys.databases WHERE name =DB_NAME()"""
        )
        res = cur.fetchone()
        assert res is not None
        return res[0]
