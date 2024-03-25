from pathlib import Path
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from deltalake import DeltaTable
from datetime import date

from odbc2deltalake.query import sql_quote_value

if TYPE_CHECKING:
    from tests.conftest import DB_Connection


@pytest.mark.order(13)
def test_delta_sys(connection: "DB_Connection"):
    from odbc2deltalake import write_db_to_delta, DBDeltaPathConfigs, WriteConfig

    cfg = WriteConfig(load_mode="simple_delta")

    base_path = Path("tests/_data/dbo/company3")
    write_db_to_delta(
        connection.conn_str, ("dbo", "company3"), base_path, cfg
    )  # full load
    t = DeltaTable(base_path / "delta")
    col_names = [f.name for f in t.schema().fields]
    assert "__timestamp" in col_names
    with connection.new_connection() as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
insert into dbo.[company3](id, name)
select 'c300',
    'The 300 company';
    UPDATE dbo.[company3] SET name='Die zwooti firma' where id='c2';
                   """
            )

    write_db_to_delta(
        connection.conn_str, ("dbo", "company3"), base_path, cfg
    )  # delta load
    t.update_incremental()
    col_names = [f.name for f in t.schema().fields]
    assert "__timestamp" in col_names
    with nc.cursor() as cursor:
        cursor.execute("SELECT * FROM [dbo].[company3]")
        alls = cursor.fetchall()
        print(alls)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, DeltaTable(base_path / "delta"), "v_company_scd2"
        )
        name_tuples = con.execute(
            """SELECT lf.name from v_company_scd2 lf 
                qualify row_number() over (partition by lf."id" order by lf."Start" desc)=1
                order by lf."id" 
                """
        ).fetchall()
        assert name_tuples == [
            ("The First company",),
            ("Die zwooti firma",),
            ("The 300 company",),
        ]
    import time

    time.sleep(1)
    write_db_to_delta(
        connection.conn_str, ("dbo", "company3"), base_path
    )  # if you switch to full delta later on, that's ok. It will just recalc the latest_pk thing
