from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import get_sql_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date


if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(6)
@pytest.mark.asyncio
async def test_strange_delta(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs

    base_path = Path("tests/_data/dbo/user3")
    await write_db_to_delta(
        connection.conn_str, ("dbo", "user3"), base_path, connection.conn
    )
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
    await write_db_to_delta(
        connection.conn_str, ("dbo", "user3"), base_path, connection.conn
    )
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

    await write_db_to_delta(
        connection.conn_str, ("dbo", "user3"), base_path, connection.conn
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
                                inner join v_user3_scd2 s2 on s2."User - iD"=lf."User - iD" and s2."time stämp"=lf."time stämp"
                where not s2.__is_deleted
                qualify row_number() over (partition by s2."User - iD" order by lf."time stämp" desc)=1
                    
                order by s2."User - iD" 
                """
        ).fetchall()
        assert id_tuples == [
            ("Vreni", "Anders", "c1"),
            ("Peter", "Johniingham", "c2"),
            ("Petra", "wayne", "c1"),  # the update on user3 is undone by the rename
            # ("Markus", "Müller"), # these inserts are not undone by the rename
            # ("Heiri", "Meier"),
        ]
