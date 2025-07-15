from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import sqlglot as sg
import sqlglot.expressions as ex
from odbc2deltalake.query import sql_quote_value

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(15)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta_query(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import DBDeltaPathConfigs, WriteConfig

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user5")[
        conf_name
    ]
    if reader.source_dialect == "postgres":
        query = sg.parse_one(
            "select *, xmin from dbo.user5 where Age is null or age < 50",
            dialect="postgres",
        )
        delta_col = "xmin"
    else:
        query = sg.parse_one(
            "select * from dbo.user5 where Age is null or age < 50", dialect="tsql"
        )
        delta_col = "time stamp"
    ts_col = delta_col.replace(" ", "_").lower()
    config = WriteConfig(
        primary_keys=["User_-_iD"],
        delta_col=delta_col,
        dialect=reader.source_dialect,
    )
    assert isinstance(query, ex.Query)
    write_db_to_delta_with_check(reader, query, dest, write_config=config)
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute(
                """INSERT INTO dbo.user5 (FirstName, LastName, Age, companyid)
                   SELECT 'Markus', 'Müller', 27, 'c2'
                   union all 
                   select 'Heiri', 'Meier', 60.98, 'c2'"""
            )
            cursor.execute("DELETE FROM dbo.user5 where LastName='Anders'")
            cursor.execute(
                "UPDATE dbo.user5 SET LastName='wayne-hösch' where LastName='wayne' -- Petra"
            )
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM dbo.user5")
            alls = cursor.fetchall()
            print(alls)

    import time

    time.sleep(2)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_5_temp",
            use_delta_ext=conf_name == "spark",
        )
        res = con.execute("select max(__timestamp) from v_user_5_temp s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None

    write_db_to_delta_with_check(reader, query, dest, write_config=config)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_scd2",
            use_delta_ext=conf_name == "spark",
        )

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
        ]

        name_tuples2 = con.execute(
            f"""SELECT FirstName, LastName  from v_user_scd2 
                where __timestamp>{sql_quote_value(max_valid_from)} and not __is_deleted
                order by "User_-_iD", __timestamp"""
        ).fetchall()
        assert name_tuples2 == [
            ("Petra", "wayne-hösch"),
            ("Markus", "Müller"),
        ]

        duckdb_create_view_for_delta(
            con,
            (
                dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).as_delta_table(),
            "v_latest_pk",
            use_delta_ext=conf_name == "spark",
        )

        id_tuples = con.execute(
            f"""SELECT s2.FirstName, s2.LastName from v_latest_pk lf 
                                inner join v_user_scd2 s2 on s2."User_-_iD"=lf."User_-_iD" and s2."{ts_col}"=lf."{ts_col}"
                qualify row_number() over (partition by s2."User_-_iD" order by lf."{ts_col}" desc)=1
                order by s2."User_-_iD" 
                """
        ).fetchall()
        assert id_tuples == [
            ("Peter", "Johniingham"),
            ("Petra", "wayne-hösch"),
            ("Markus", "Müller"),
        ]
