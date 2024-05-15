from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta, duckdb_create_view_for_delta
import duckdb
from deltalake import DeltaTable
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs

from odbc2deltalake.query import sql_quote_value

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(5)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake.reader.odbc_reader import ODBCReader
    from odbc2deltalake import DBDeltaPathConfigs

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user2")[
        conf_name
    ]
    write_db_to_delta_with_check(reader, ("dbo", "user2$"), dest)
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO [dbo].[user2$] ([FirstName], [LastName], [Age], companyid)
                   SELECT 'Markus', 'Müller', 27, 'c2'
                   union all 
                   select 'Heiri', 'Meier', 27.98, 'c2';
                   DELETE FROM dbo.[user2$] where LastName='Anders';
                     UPDATE [dbo].[user2$] SET LastName='wayne-hösch' where LastName='wayne'; -- Petra
                   """
            )
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[user2$]")
            alls = cursor.fetchall()
            cols = [c[0] for c in cursor.description]
            dicts = [dict(zip(cols, row)) for row in alls]
            print(alls)

    import time

    time.sleep(2)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, (dest / "delta").as_delta_table(), "v_user_2_temp"
        )
        res = con.execute("select max(__timestamp) from v_user_2_temp s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None

    write_db_to_delta_with_check(
        reader,
        ("dbo", "user2$"),
        dest,
    )
    with duckdb.connect() as con:
        sql = get_sql_for_delta((dest / "delta").as_delta_table())
        assert sql is not None
        con.execute("CREATE VIEW v_user_scd2 AS " + sql)

        name_tuples = con.execute(
            'SELECT FirstName, LastName, __is_deleted  from v_user_scd2 order by "User_-_iD", __timestamp'
        ).fetchall()
        assert name_tuples == [
            ("John", "Anders", False),
            (None, None, True),
            ("Peter", "Johniingham", False),
            ("Petra", "wayne", False),
            ("Petra", "wayne-hösch", False),
            ("Markus", "Müller", False),
            ("Heiri", "Meier", False),
        ]

        name_tuples2 = con.execute(
            f"""SELECT FirstName, LastName  from v_user_scd2 
                where __timestamp>{sql_quote_value(max_valid_from)} and not __is_deleted
                order by "User_-_iD", __timestamp"""
        ).fetchall()
        assert name_tuples2 == [
            ("Petra", "wayne-hösch"),
            ("Markus", "Müller"),
            ("Heiri", "Meier"),
        ]
        duckdb_create_view_for_delta(
            con,
            (
                dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).as_delta_table(),
            "v_latest_pk",
        )

        id_tuples = con.execute(
            """SELECT s2.FirstName, s2.LastName from v_latest_pk lf 
                                inner join v_user_scd2 s2 on s2."User_-_iD"=lf."User_-_iD" and s2."time_stamp"=lf."time_stamp"
                qualify row_number() over (partition by s2."User_-_iD" order by lf."time_stamp" desc)=1
                order by s2."User_-_iD" 
                """
        ).fetchall()
        assert id_tuples == [
            ("Peter", "Johniingham"),
            ("Petra", "wayne-hösch"),
            ("Markus", "Müller"),
            ("Heiri", "Meier"),
        ]


@pytest.mark.order(5)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta_sys(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import DBDeltaPathConfigs

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/company_2")[
        conf_name
    ]
    write_db_to_delta_with_check(reader, ("dbo", "company"), dest)  # full load
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
insert into dbo.[company](id, name)
select 'c300',
    'The 300 company';
update dbo.[company]
set id='c2 '
    where id='c2'
                   """
            )

    write_db_to_delta_with_check(reader, ("dbo", "company"), dest)  # delta load
    with nc.cursor() as cursor:
        cursor.execute("SELECT * FROM [dbo].[company]")
        alls = cursor.fetchall()
        print(alls)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, (dest / "delta").as_delta_table(), "v_company_scd2"
        )

        duckdb_create_view_for_delta(
            con,
            (
                dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).as_delta_table(),
            "v_latest_pk",
        )
        name_tuples = con.execute(
            """SELECT lf.id, lf.name from v_company_scd2 lf 
                                inner join v_latest_pk s2 on s2."id"=lf."id" and s2."SysStartTime"=lf."SysStartTime"
                qualify row_number() over (partition by s2."id" order by lf."SysStartTime" desc)=1
                order by s2."id" 
                """
        ).fetchall()
        assert name_tuples == [
            (
                "c1",
                "The First company",
            ),
            (
                "c2",  # we don't accept spaces
                "The Second company",
            ),
            (
                "c300",
                "The 300 company",
            ),
        ]
