from pathlib import Path
import time
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date
from .utils import write_db_to_delta_with_check
import sqlglot.expressions as ex

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(10)
def test_first_load_timestamp(connection: "DB_Connection"):
    from odbc2deltalake import (
        write_db_to_delta,
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
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute("""DROP TABLE IF EXISTS dbo.User_Double""")
            cursor.execute("""SELECT * INTO dbo.User_Double FROM dbo.[User] """)
            cursor.execute(
                """ALTER TABLE dbo.User_Double ADD PRIMARY KEY ([User - iD]) """
            )
    base_path = Path("tests/_data/dbo/user_double")
    write_db_to_delta_with_check(
        connection.conn_str,
        ("dbo", "User_Double"),
        base_path,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_user AS " + sql)

        name_tuples = con.execute(
            'SELECT FirstName from v_user order by "User_-_iD"'
        ).fetchall()
        assert name_tuples == [("John",), ("Peter",), ("Petra",)]
        age_dt = con.execute(
            "select data_type from information_Schema.columns where table_name='v_user' and column_name='Age'"
        ).fetchall()[0]
        assert age_dt[0].upper() == "DOUBLE"
        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
        )

        id_tuples = con.execute(
            'SELECT "User_-_iD" from v_latest_pk order by "User_-_iD"'
        ).fetchall()
        assert id_tuples == [(1,), (2,), (3,)]

    time.sleep(1)
    with connection.new_connection() as nc:
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
        connection.conn_str,
        ("dbo", "User_Double"),
        base_path,
        write_config=write_config,
    )
