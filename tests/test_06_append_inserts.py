import time
from typing import TYPE_CHECKING

import duckdb
import pytest
import sqlglot as sg

from tests.utils import config_names, create_delta_view, get_test_run_configs

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from tests.conftest import DBConnection


@pytest.mark.order(11)
@pytest.mark.parametrize("conf_name", config_names)
def test_append_inserts(connection: "DBConnection", spark_session: "SparkSession", conf_name: str):
    from odbc2deltalake import (
        WriteConfig,
        write_db_to_delta,
    )

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/log")[conf_name]

    write_config = WriteConfig(load_mode="append_inserts", dialect=reader.source_dialect)
    write_db_to_delta(
        reader,
        ("dbo", "log"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        create_delta_view(
            con,
            dest / "delta",
            "v_log",
            use_delta_ext=conf_name == "spark",
        )

        id_tuples = con.execute('SELECT id, message from v_log order by "__timestamp"').fetchall()
        assert id_tuples == [(1, "The first log message")]

    time.sleep(1)
    with connection.new_connection(conf_name) as nc, nc.cursor() as cursor:
        cursor.execute(
            sg.parse_one(
                """INSERT INTO [dbo].[log] ([message])
                   SELECT 'The second log message'""",
                dialect="tsql",
            ).sql(reader.source_dialect)
        )
    write_db_to_delta(  # some delta load
        reader,
        ("dbo", "log"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        create_delta_view(
            con,
            dest / "delta",
            "v_log",
            use_delta_ext=conf_name == "spark",
        )

        id_tuples = con.execute('SELECT id, message from v_log order by "__timestamp"').fetchall()
        assert id_tuples == [
            (1, "The first log message"),
            (2, "The second log message"),
        ]
