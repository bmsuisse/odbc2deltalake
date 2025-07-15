from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import sqlglot as sg
from odbc2deltalake.query import sql_quote_value

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(5)
@pytest.mark.parametrize("conf_name", config_names)
def test_delta(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import DBDeltaPathConfigs
    import sqlglot.expressions as ex

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user2")[
        conf_name
    ]
    write_db_to_delta_with_check(reader, ("dbo", "user2$"), dest)
    fields = reader.get_local_delta_ops(dest / "delta").column_infos()
    nbr_field = next(f for f in fields if f.column_name == "nbr")
    assert nbr_field.data_type.this == ex.DataType.Type.SMALLINT
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            parsed = sg.parse(
                """INSERT INTO [dbo].[user2$] (FirstName, LastName, Age, companyid)
                   SELECT 'Markus', 'Müller', 27, 'c2'
                   union all 
                   select 'Heiri', 'Meier', 27.98, 'c2';
                   DELETE FROM dbo.[user2$] where LastName='Anders';
                     UPDATE [dbo].[user2$] SET LastName='wayne-hösch' where LastName='wayne' -- Petra
                   """,
                dialect="tsql",
            )
            for p in parsed:
                assert p is not None
                cursor.execute(p.sql(reader.source_dialect))
            cursor.execute("COMMIT")
        with nc.cursor() as cursor:
            cursor.execute(
                sg.parse_one("SELECT * FROM [dbo].[user2$]", dialect="tsql").sql(
                    reader.source_dialect
                )
            )
            alls = cursor.fetchall()
            assert cursor.description is not None
            cols = [c[0] for c in cursor.description]
            dicts = [dict(zip(cols, row)) for row in alls]
            print(alls)

    import time

    time.sleep(2)
    ts_col = "xmin" if reader.source_dialect == "postgres" else "time_stamp"
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_2_temp",
            use_delta_ext=conf_name == "spark",
        )
        if reader.source_dialect in ["tsql", "mssql"]:
            res = con.execute(
                'select "time_stamp", "User_-_iD" from v_user_2_temp limit 1'
            ).fetchone()
            assert res is not None
            assert isinstance(res[0], int), "time_stamp is not an integer"

        res = con.execute("select max(__timestamp) from v_user_2_temp s").fetchone()
        assert res is not None
        max_valid_from = res[0]
        assert max_valid_from is not None

    _, l2 = write_db_to_delta_with_check(
        reader,
        ("dbo", "user2$"),
        dest,
    )
    assert l2.executed_type == "delta"
    assert not l2.dirty
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
    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            stmts = sg.parse(
                """
insert into dbo.[company](id, name)
select 'c300',
    'The 300 company';
                   """,
                dialect="tsql",
            )
            for stmt in stmts:
                assert stmt is not None
                print(stmt.sql(reader.source_dialect))
                cursor.execute(stmt.sql(reader.source_dialect))
            if reader.source_dialect in ["tsql", "mssql"]:
                cursor.execute("""
update dbo.[company]
set id='c2 '
    where id='c2'""")  # postgres fails here, which is actually correct, since c2 is referenced by FK

        write_db_to_delta_with_check(reader, ("dbo", "company"), dest)  # delta load
        with nc.cursor() as cursor:
            cursor.execute("SELECT * FROM dbo.company")
            alls = cursor.fetchall()
            print(alls)
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_company_scd2",
            use_delta_ext=conf_name == "spark",
        )

        duckdb_create_view_for_delta(
            con,
            (
                dest / "delta_load" / DBDeltaPathConfigs.LATEST_PK_VERSION
            ).as_delta_table(),
            "v_latest_pk",
            use_delta_ext=conf_name == "spark",
        )
        delta_col = "xmin" if reader.source_dialect == "postgres" else "SysStartTime"
        name_tuples = con.execute(
            f"""SELECT lf.id, lf.name from v_company_scd2 lf 
                                inner join v_latest_pk s2 on s2."id"=lf."id" and s2."{delta_col}"=lf."{delta_col}"
                qualify row_number() over (partition by s2."id" order by lf."{delta_col}" desc)=1
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
