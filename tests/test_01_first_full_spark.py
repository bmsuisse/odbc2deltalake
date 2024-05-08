from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date
from .utils import write_db_to_delta_with_check
from odbc2deltalake.reader.spark_reader import SparkReader
from odbc2deltalake import DBDeltaPathConfigs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(1)
def test_first_load_timestamp(
    connection: "DB_Connection", spark_session: "SparkSession"
):
    reader = SparkReader(spark_session, connection.jdbc_options, jdbc=True)
    base_path = Path("tests/_data/spark/dbo/user")
    write_db_to_delta_with_check(reader, ("dbo", "user"), base_path)
    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_user AS " + sql)

        name_tuples = con.execute(
            'SELECT FirstName from v_user order by "User_-_iD"'
        ).fetchall()
        assert name_tuples == [("John",), ("Peter",), ("Petra",)]

        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
        )

        id_tuples = con.execute(
            'SELECT "User_-_iD" from v_latest_pk order by "User_-_iD"'
        ).fetchall()
        assert id_tuples == [(1,), (2,), (3,)]


@pytest.mark.order(2)
def test_first_load_sys_start(
    connection: "DB_Connection", spark_session: "SparkSession"
):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    reader = SparkReader(spark_session, connection.jdbc_options, jdbc=True)

    base_path = Path("tests/_data/spark/dbo/company")

    write_db_to_delta(reader, ("dbo", "company"), base_path)

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
def test_first_load_always_full(
    connection: "DB_Connection", spark_session: "SparkSession"
):
    from odbc2deltalake import write_db_to_delta

    reader = SparkReader(spark_session, connection.jdbc_options, jdbc=True)

    base_path = Path(
        "tests/_data/spark/long_schema/long_table_name"
    )  # spaces in file names cause trouble with delta-rs
    write_db_to_delta(
        reader,
        ("long schema", "long table name"),
        base_path,
    )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_long_table_name AS " + sql)

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
