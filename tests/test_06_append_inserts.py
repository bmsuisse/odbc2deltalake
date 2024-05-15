from pathlib import Path
import time
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from deltalake import DeltaTable
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(11)
@pytest.mark.parametrize("conf_name", config_names)
def test_append_inserts(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import (
        write_db_to_delta,
        WriteConfig,
    )

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/log")[conf_name]

    write_config = WriteConfig(load_mode="append_inserts")
    write_db_to_delta(
        reader,
        ("dbo", "log"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, (dest / "delta").as_delta_table(), "v_log")

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
        reader,
        ("dbo", "log"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, (dest / "delta").as_delta_table(), "v_log")

        id_tuples = con.execute(
            'SELECT id, message from v_log order by "__timestamp"'
        ).fetchall()
        assert id_tuples == [
            (1, "The first log message"),
            (2, "The second log message"),
        ]
