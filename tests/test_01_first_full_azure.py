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

    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs, AzureDestination
    from deltalake2db.duckdb import apply_storage_options

    destination = AzureDestination("testlakeodbc", "dbo/user", {"use_emulator": "true"})
    await write_db_to_delta(
        connection.conn_str,
        ("dbo", "user"),
        destination,
    )

    with duckdb.connect() as con:
        apply_storage_options(con, destination.storage_options)
        sql = get_sql_for_delta((destination / "delta").as_delta_table())
        assert sql is not None
        con.execute("CREATE VIEW v_user AS " + sql)

        name_tuples = con.execute(
            'SELECT FirstName from v_user order by "User - iD"'
        ).fetchall()
        assert name_tuples == [("John",), ("Peter",), ("Petra",)]

        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta((destination / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION).as_delta_table()) }'
        )

        id_tuples = con.execute(
            'SELECT "User - iD" from v_latest_pk order by "User - iD"'
        ).fetchall()
        assert id_tuples == [(1,), (2,), (3,)]
