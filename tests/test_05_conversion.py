import time
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb

from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import sqlglot.expressions as ex

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(10)
@pytest.mark.parametrize("conf_name", config_names)
def test_first_load_timestamp(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import (
        DBDeltaPathConfigs,
        WriteConfig,
        DEFAULT_DATA_TYPE_MAP,
    )

    write_config = WriteConfig(
        data_type_map={
            "decimal": ex.DataType(this=ex.DataType.Type.DOUBLE),
            "numeric": ex.DataType(this=ex.DataType.Type.DOUBLE),
        }
        | dict(DEFAULT_DATA_TYPE_MAP)
    )
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_double")[
        conf_name
    ]
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("""DROP TABLE IF EXISTS dbo.User_Double""")
            cursor.execute("""SELECT * INTO dbo.User_Double FROM dbo.[User] """)
            cursor.execute(
                """ALTER TABLE dbo.User_Double ADD PRIMARY KEY ([User - iD]) """
            )
    write_db_to_delta_with_check(
        reader,
        ("dbo", "User_Double"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, (dest / "delta").as_delta_table(), "v_user")

        name_tuples = con.execute(
            'SELECT FirstName from v_user order by "User_-_iD"'
        ).fetchall()
        assert name_tuples == [("John",), ("Peter",), ("Petra",)]
        age_dt = con.execute(
            "select data_type from information_Schema.columns where table_name='v_user' and column_name='Age'"
        ).fetchall()[0]
        assert age_dt[0].upper() == "DOUBLE"
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

    time.sleep(1)
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO [dbo].[User_Double] ([FirstName], [LastName], [Age], companyid)
                   SELECT 'Markus', 'Müller', 27, 'c2'
                   union all 
                   select 'Heiri', 'Meier', 27.98, 'c2';
                   DELETE FROM dbo.[User_Double] where LastName='Anders';
                     UPDATE [dbo].[User_Double] SET LastName='wayne-hösch' where LastName='wayne'; -- Petra
                   """
            )
    write_db_to_delta_with_check(  # some delta load
        reader,
        ("dbo", "User_Double"),
        dest,
        write_config=write_config,
    )
