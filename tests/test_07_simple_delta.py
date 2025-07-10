from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs


if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(13)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta_sys(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import write_db_to_delta, WriteConfig

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/company3")[
        conf_name
    ]

    cfg = WriteConfig(load_mode="simple_delta")
    write_db_to_delta_with_check(reader, ("dbo", "company3"), dest, cfg)  # full load
    t = reader.get_local_delta_ops(dest / "delta")
    col_names = [f.column_name for f in t.column_infos()]
    assert "__timestamp" in col_names
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """
insert into dbo.[company3](id, name)
select 'c300',
    'The 300 company';
    UPDATE dbo.[company3] SET name='Die zwooti firma' where id='c2';
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
            con,
            (dest / "delta").as_delta_table(),
            "v_company_scd2",
            use_delta_ext=conf_name == "spark",
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
    write_db_to_delta_with_check(
        reader, ("dbo", "company3"), dest
    )  # if you switch to full delta later on, that's ok. It will just recalc the latest_pk thing
