from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date

from odbc2deltalake.query import sql_quote_value

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(5)
def test_delta(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path("tests/_data/dbo/user2")
    write_db_to_delta(connection.conn_str, ("dbo", "user2"), base_path)
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO [dbo].[user2] ([FirstName], [LastName], [Age], companyid)
                   SELECT 'Markus', 'Müller', 27, 'c2'
                   union all 
                   select 'Heiri', 'Meier', 27.98, 'c2';
                   DELETE FROM dbo.[user2] where LastName='Anders';
                     UPDATE [dbo].[user2] SET LastName='wayne-hösch' where LastName='wayne'; -- Petra
                   """
            )
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[user2]")
            alls = cursor.fetchall()
            print(alls)

    import time

    time.sleep(2)
    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        res = con.execute("select max(__timestamp) from (" + sql + ") s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None

    write_db_to_delta(connection.conn_str, ("dbo", "user2"), base_path)
    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
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

        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
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
def test_delta_sys(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path("tests/_data/dbo/company2")
    write_db_to_delta(connection.conn_str, ("dbo", "company"), base_path)  # full load
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
insert into dbo.[company](id, name)
select 'c300',
    'The 300 company';
                   """
            )

    write_db_to_delta(connection.conn_str, ("dbo", "company"), base_path)  # delta load
    with nc.cursor() as cursor:
        cursor.execute("SELECT * FROM [dbo].[company]")
        alls = cursor.fetchall()
        print(alls)
    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE VIEW v_company_scd2 AS " + sql)

        con.execute(
            f'create view v_latest_pk as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
        )
        name_tuples = con.execute(
            """SELECT lf.name from v_company_scd2 lf 
                                inner join v_latest_pk s2 on s2."id"=lf."id" and s2."SysStartTime"=lf."SysStartTime"
                qualify row_number() over (partition by s2."id" order by lf."SysStartTime" desc)=1
                order by s2."id" 
                """
        ).fetchall()
        assert name_tuples == [
            ("The First company",),
            ("The Second company",),
            ("The 300 company",),
        ]
