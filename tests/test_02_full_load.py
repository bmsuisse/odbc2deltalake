from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date

from odbc2deltalake.query import sql_quote_value


if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(4)
@pytest.mark.asyncio
async def test_first_load_always_full(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path(
        "tests/_data/long_schema/long_table_name2"
    )  # spaces in file names cause trouble with delta-rs

    await write_db_to_delta(
        connection.conn_str,
        ("long schema", "long table name"),
        base_path,
    )
    import time

    time.sleep(2)
    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        res = con.execute("select max(__valid_from) from (" + sql + ") s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None

    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO [long schema].[long table name] ([long column name], dt, [date])
    SELECT 5,
        '<root><child>text</child></root>',
        '2025-01-01'"""
            )

    await write_db_to_delta(
        connection.conn_str,
        ("long schema", "long table name"),
        base_path,
    )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_long_table_name AS " + sql)

        name_tuples = con.execute(
            f'SELECT date from v_long_table_name where __valid_from>{sql_quote_value(max_valid_from)} order by "long column name"'
        ).fetchall()
        assert name_tuples == [
            (
                date(
                    2023,
                    1,
                    1,
                ),
            ),
            (
                date(
                    2024,
                    1,
                    1,
                ),
            ),
            (
                date(
                    2025,
                    1,
                    1,
                ),
            ),
        ]
