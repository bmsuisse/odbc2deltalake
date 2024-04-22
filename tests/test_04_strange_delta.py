from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta, duckdb_create_view_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date
from .utils import write_db_to_delta_with_check


if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(6)
def test_strange_delta(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs
    from odbc2deltalake.reader.odbc_reader import ODBCReader

    base_path = Path("tests/_data/dbo/user3")
    write_db_to_delta_with_check(connection.conn_str, ("dbo", "user3"), base_path)
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
                    UPDATE dbo.[user4] SET FirstName='Vreni' where LastName='Anders';
                    
                    -- this update is later a "strange" one
                    UPDATE dbo.[user4] SET companyid='c2' where LastName='Johniingham';

                    INSERT INTO [dbo].[user3] ([FirstName], [LastName], [Age], companyid)
                   SELECT 'Markus', 'Müller', 27, 'c2'
                   union all 
                   select 'Heiri', 'Meier', 27.98, 'c2';
                   DELETE FROM dbo.[user3] where LastName='Anders';
                     UPDATE [dbo].[user3] SET LastName='wayne-hösch' where LastName='wayne'; -- Petra
                   """
            )
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[user3]")
            alls = cursor.fetchall()
            print(alls)
    import time

    time.sleep(2)
    write_db_to_delta_with_check(connection.conn_str, ("dbo", "user3"), base_path)
    # so far we have no strange data yet. But we make it happen ;)
    # we rename user4 to user3, which will through around timestamps especially for record Johniingham
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """exec sp_rename 'dbo.user3', 'user3_';
                   exec sp_rename 'dbo.user4', 'user3';
                   """
            )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        res = con.execute("select max(__timestamp) from (" + sql + ") s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute("""select * from user3""")
            alls = cursor.fetchall()
            cols = [c[0] for c in cursor.description]
            dicts = [dict(zip(cols, row)) for row in alls]
            print(dicts)
    write_db_to_delta_with_check(
        ODBCReader(connection.conn_str, "tests/_data/debug_user2.duck"),
        ("dbo", "user3"),
        base_path,
    )

    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        con.execute("CREATE OR REPLACE VIEW v_user3_scd2 AS " + sql)

        con.execute(
            f'create view v_latest_pk_user3 as {get_sql_for_delta(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION) }'
        )

        id_tuples = con.execute(
            """SELECT s2.FirstName, s2.LastName, companyid from v_latest_pk_user3 lf 
                                inner join v_user3_scd2 s2 on s2."User_-_iD"=lf."User_-_iD" and s2."time_stamp"=lf."time_stamp"
                where not s2.__is_deleted
                qualify row_number() over (partition by s2."User_-_iD" order by lf."time_stamp" desc)=1
                    
                order by s2."User_-_iD" 
                """
        ).fetchall()
        assert id_tuples == [
            ("Vreni", "Anders", "c1"),
            ("Peter", "Johniingham", "c2"),
            ("Petra", "wayne", "c1"),  # the update on user3 is undone by the rename
            # ("Markus", "Müller"), # these inserts are not undone by the rename
            # ("Heiri", "Meier"),
        ]


@pytest.mark.order(7)
def test_strange_delta_sys(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    import time

    base_path = Path("tests/_data/dbo/company2_1")
    write_db_to_delta(connection.conn_str, ("dbo", "company2"), base_path)  # empty
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """ insert into dbo.company2(id, name) select id, name from dbo.company where id <> 'c300'; """
            )
    write_db_to_delta_with_check(  # normal full load
        connection.conn_str, ("dbo", "company2"), base_path
    )

    time.sleep(2)
    with duckdb.connect() as con:
        sql = get_sql_for_delta(DeltaTable(base_path / "delta"))
        assert sql is not None
        res = con.execute("select max(__timestamp) from (" + sql + ") s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None
    time.sleep(2)
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute("ALTER TABLE dbo.company2 drop PERIOD FOR SYSTEM_TIME;")
        with nc.cursor() as cursor:
            cursor.execute(
                """ -- let's manipulate the history
                    
                    INSERT INTO dbo.company2(id, name, SysStartTime, SysEndTime)
                    select 'c299', 'The 299th company', '2022-01-01', (select max(SysEndTime) from dbo.company2);
                    UPDATE dbo.company2
                        set SysStartTime='2000-01-01', 
                            name = 'The 1 company - renamed'
                        where id='c1';
                   """
            )
        with nc.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE dbo.company2 add PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)  "
            )
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM [dbo].[company2]")
            alls = cursor.fetchall()
            cols = [c[0] for c in cursor.description]
            dicts = [dict(zip(cols, row)) for row in alls]
            print(dicts)
    write_db_to_delta_with_check(connection.conn_str, ("dbo", "company2"), base_path)

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, DeltaTable(base_path / "delta"), "v_company2_scd2"
        )
        duckdb_create_view_for_delta(
            con,
            DeltaTable(base_path / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION),
            "v_latest_pk_company2",
        )

        id_tuples = con.execute(
            """SELECT s2.id, s2.name from v_latest_pk_company2 lf 
                                inner join v_company2_scd2 s2 on s2."id"=lf."id" and s2."SysStartTime"=lf."SysStartTime"
                where not s2.__is_deleted
                qualify row_number() over (partition by s2."id" order by lf."SysStartTime" desc)=1
                    
                order by s2."id" 
                """
        ).fetchall()
        assert id_tuples == [
            ("c1", "The 1 company - renamed"),
            ("c2", "The Second company"),
            (
                "c299",
                "The 299th company",
            ),
        ]
