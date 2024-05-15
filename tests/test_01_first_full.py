from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(1)
@pytest.mark.parametrize("conf_name", config_names)
def test_first_load_timestamp(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import DBDeltaPathConfigs

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user")[
        conf_name
    ]

    write_db_to_delta_with_check(reader, ("dbo", "user"), dest)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, (dest / "delta").as_delta_table(), "v_user")
        name_tuples = con.execute(
            'SELECT FirstName from v_user order by "User_-_iD"'
        ).fetchall()
        assert name_tuples == [("John",), ("Peter",), ("Petra",)]

        duckdb_create_view_for_delta(
            con,
            (
                dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).as_delta_table(),
            "v_latest_pk",
        )

        id_tuples = con.execute(
            'SELECT "User_-_iD" from v_latest_pk order by "User_-_iD"'
        ).fetchall()
        assert id_tuples == [(1,), (2,), (3,)]


@pytest.mark.order(2)
@pytest.mark.parametrize("conf_name", config_names)
def test_first_load_sys_start(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import DBDeltaPathConfigs, write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/company")[
        conf_name
    ]

    write_db_to_delta(reader, ("dbo", "company"), dest)

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, (dest / "delta").as_delta_table(), "v_company"
        )
        name_tuples = con.execute('SELECT name from v_company order by "id"').fetchall()
        assert name_tuples == [("The First company",), ("The Second company",)]

        duckdb_create_view_for_delta(
            con,
            (
                dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).as_delta_table(),
            "v_latest_pk_company",
        )
        id_tuples = con.execute(
            'SELECT "id" from v_latest_pk_company order by "id"'
        ).fetchall()
        assert id_tuples == [("c1",), ("c2",)]


@pytest.mark.order(3)
def test_first_load_always_full(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(
        connection, spark_session, "long_schema/long_table_name"
    )[conf_name]

    write_db_to_delta(
        reader,
        ("long schema", "long table name"),
        dest,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, (dest / "delta").as_delta_table(), "v_long_table_name"
        )

        name_tuples = con.execute(
            'SELECT date from v_long_table_name order by "long_column_name"'
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
