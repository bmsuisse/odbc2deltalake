from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import time

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(14)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta_sys(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import write_db_to_delta, WriteConfig

    cfg = WriteConfig(load_mode="simple_delta_check")
    reader, dest = get_test_run_configs(connection, spark_session, "dbo/company_3_dc")[
        conf_name
    ]

    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest, cfg)  # full load
    t = reader.get_local_delta_ops((dest / "delta"))

    col_names = [f.column_name for f in t.column_infos()]
    assert "__timestamp" in col_names
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
insert into dbo.[company3](id, name)
select 'c400',
    'The 400 company';
    UPDATE dbo.[company3] SET name='Die zwooti firma 2.0', date_timer=getdate() where id='c2';
    
insert into dbo.[company3](id, name)
select 'c500',
    'The 500 company';
                   """
            )

    write_db_to_delta(reader, ("dbo", "company3"), dest, cfg)  # delta load
    t.update_incremental()
    col_names = [f.column_name for f in t.column_infos()]
    assert "__timestamp" in col_names
    with nc.cursor() as cursor:
        cursor.execute("SELECT * FROM [dbo].[company3]")
        alls = cursor.fetchall()
        print(alls)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, (dest / "delta").as_delta_table(), "v_company_scd2"
        )
        name_tuples = con.execute(
            """SELECT lf.name, lf.__is_deleted from v_company_scd2 lf 
            where lf.id in ('c1', 'c2', 'c400', 'c500')
                qualify row_number() over (partition by lf."id" order by lf."Start" desc)=1
                order by lf."id" 
                """
        ).fetchall()
        assert name_tuples == [
            ("The First company", False),
            ("Die zwooti firma 2.0", False),
            ("The 400 company", False),
            ("The 500 company", False),
        ]

    time.sleep(1)
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
delete from dbo.[company3] where id='c400'
                   """
            )
    time.sleep(1)
    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest, write_config=cfg)

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_company_scd2",
            use_delta_ext=conf_name == "spark",
        )
        name_tuples = con.execute(
            """SELECT lf.name, lf.__is_deleted from v_company_scd2 lf 
            where lf.id in ('c1', 'c2', 'c400', 'c500')
                qualify row_number() over (partition by lf."id" order by lf."__timestamp" desc)=1
                order by lf."id" 
                """
        ).fetchall()
        assert name_tuples == [
            ("The First company", False),
            ("Die zwooti firma 2.0", False),
            (None, True),
            ("The 500 company", False),
        ]
    time.sleep(1)
    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest)
