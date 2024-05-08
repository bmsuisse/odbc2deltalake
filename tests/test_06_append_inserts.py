from pathlib import Path
import time
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from deltalake import DeltaTable

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(11)
def test_append_inserts(connection: "DB_Connection"):
    from odbc2deltalake import (
        write_db_to_delta,
        WriteConfig,
    )

    write_config = WriteConfig(load_mode="append_inserts")
    base_path = Path("tests/_data/dbo/log")
    write_db_to_delta(
        connection.conn_str,
        ("dbo", "log"),
        base_path,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, DeltaTable(base_path / "delta"), "v_log")

        id_tuples = con.execute(
            'SELECT id, message from v_log order by "__timestamp"'
        ).fetchall()
        assert id_tuples == [(1, "The first log message")]

    time.sleep(1)
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO [dbo].[log] ([message])
                   SELECT 'The second log message'"""
            )
    write_db_to_delta(  # some delta load
        connection.conn_str,
        ("dbo", "log"),
        base_path,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, DeltaTable(base_path / "delta"), "v_log")

        id_tuples = con.execute(
            'SELECT id, message from v_log order by "__timestamp"'
        ).fetchall()
        assert id_tuples == [
            (1, "The first log message"),
            (2, "The second log message"),
        ]
