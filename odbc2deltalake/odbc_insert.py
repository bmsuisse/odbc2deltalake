import pyodbc
import pyarrow as pa
from typing import Union, Tuple

from .sql_schema import FieldWithType
from .query import sql_quote_name

MINIMAL_JSON_COMPATIBILITY_LEVEL = 130


async def pyodbc_insert_into_table(
    *,
    reader: pa.RecordBatchReader,
    table_name: Union[Tuple[str, str], str],
    connection: pyodbc.Connection,
    schema: list[FieldWithType],
    supports_json_insert: bool | None = None,
):
    if supports_json_insert is None:
        from .metadata import get_compatibility_level

        supports_json_insert = (
            get_compatibility_level(connection) >= MINIMAL_JSON_COMPATIBILITY_LEVEL
        )

    if not supports_json_insert:
        colnames = reader.schema.names
        cols = ", ".join([sql_quote_name(c) for c in colnames])
        quots = ", ".join(["?" for _ in colnames])
        insert_to_tmp_tbl_stmt = (
            f"INSERT INTO {sql_quote_name(table_name)}({cols}) VALUES({quots})"
        )

        for batch in reader:  # type: ignore
            with connection.cursor() as cursor:
                lsdt = batch.to_pylist()
                prms = [tuple([item[c] for c in colnames]) for item in lsdt]
                cursor.executemany(insert_to_tmp_tbl_stmt, prms)
    else:
        from .json_insert import insert_into_table_via_json_from_batches

        await insert_into_table_via_json_from_batches(
            reader=reader, table_name=table_name, connection=connection, schema=schema
        )
