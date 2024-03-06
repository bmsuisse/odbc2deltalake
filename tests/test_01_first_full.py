from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(1)
@pytest.mark.asyncio
async def test_first_load_timestamp(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path("tests/_data/dbo/user")
    await write_db_to_delta(
        connection.conn_str, ("dbo", "user"), base_path, connection.conn
    )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_user AS " + sql)

        name_tuples = con.execute(
            'SELECT FirstName from v_user order by "User - iD"'
        ).fetchall()
        assert name_tuples == [("John",), ("Peter",), ("Petra",)]

        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
        )

        id_tuples = con.execute(
            'SELECT "User - iD" from v_latest_pk order by "User - iD"'
        ).fetchall()
        assert id_tuples == [(1,), (2,), (3,)]


@pytest.mark.order(2)
@pytest.mark.asyncio
async def test_first_load_sys_start(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path("tests/_data/dbo/company")

    await write_db_to_delta(
        connection.conn_str, ("dbo", "company"), base_path, connection.conn
    )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_company AS " + sql)

        name_tuples = con.execute('SELECT name from v_company order by "id"').fetchall()
        assert name_tuples == [("The First company",), ("The Second company",)]
        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
        )

        id_tuples = con.execute('SELECT "id" from v_latest_pk order by "id"').fetchall()
        assert id_tuples == [("c1",), ("c2",)]


@pytest.mark.order(3)
@pytest.mark.asyncio
async def test_first_load_always_full(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path(
        "tests/_data/long_schema/long_table_name"
    )  # spaces in file names cause trouble with delta-rs
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
            'SELECT date from v_long_table_name order by "long column name"'
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
        ]
