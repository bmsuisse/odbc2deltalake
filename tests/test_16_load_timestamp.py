import time
from typing import TYPE_CHECKING, Any
import duckdb
import pytest
import sqlglot as sg
from deltalake2db import duckdb_create_view_for_delta
from .utils import config_names, get_test_run_configs

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession

SLICES = 2
ROWS = 40


class _SlicedSourceMixin:
    """Executes every write of a source query as several slice queries, one after the other,
    like a partitioned/parallel reader does. Each slice query runs at a different time."""

    slice_dialect: str

    def source_write_sql_to_delta(
        self, sql: str, delta_path: Any, mode: str, **kwargs: Any
    ):
        d = self.slice_dialect
        if d == "postgres":
            hash_expr = 'abs(hashtext("t"."id"::text))'
        else:
            hash_expr = "(CHECKSUM([t].[id]) & 2147483647)"
        for i in range(SLICES):
            slice_sql = (
                sg.parse_one(sql, dialect=d)
                .where(sg.condition(f"{hash_expr} % {SLICES} = {i}", dialect=d))  # type: ignore
                .sql(d)
            )
            if i > 0:
                time.sleep(1.1)
            super().source_write_sql_to_delta(  # type: ignore
                slice_sql, delta_path, mode if i == 0 else "append", **kwargs
            )


def _counts(dest, conf_name: str):
    with duckdb.connect() as con:
        use_ext = conf_name == "spark"
        duckdb_create_view_for_delta(
            con, (dest / "delta").as_delta_table(), "v_all", use_delta_ext=use_ext
        )
        duckdb_create_view_for_delta(
            con,
            (dest / "delta_load" / "latest_pk_version").as_delta_table(),
            "v_lpk",
            use_delta_ext=use_ext,
        )
        total = con.execute("SELECT count(*) FROM v_all").fetchall()[0][0]
        lpk = con.execute("SELECT count(*) FROM v_lpk").fetchall()[0][0]
        full_ts = con.execute(
            'SELECT count(DISTINCT "__timestamp") FROM v_all WHERE "__is_full_load"'
        ).fetchall()[0][0]
        return total, lpk, full_ts


@pytest.mark.order(21)
@pytest.mark.parametrize("conf_name", config_names)
def test_sliced_source_read_has_one_load_timestamp(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """__timestamp is the same for all rows of a load, even if the reader splits the source query
    into slices that run at different times. Otherwise latest_pk_version misses the rows of all
    but the last slice and the next load appends them again."""
    from odbc2deltalake import WriteConfig, write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/ts_load")[
        conf_name
    ]
    is_pg = reader.source_dialect == "postgres"
    sliced_cls = type(
        "Sliced" + type(reader).__name__, (_SlicedSourceMixin, type(reader)), {}
    )
    sliced = sliced_cls.__new__(sliced_cls)
    sliced.__dict__.update(reader.__dict__)
    sliced.slice_dialect = reader.source_dialect  # type: ignore

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS dbo.ts_load")
        with nc.cursor() as cursor:
            if is_pg:
                cursor.execute(
                    "CREATE TABLE dbo.ts_load (id int primary key, val varchar(50))"
                )
            else:
                cursor.execute(
                    "CREATE TABLE dbo.ts_load (id int primary key, val varchar(50), rv rowversion)"
                )
        with nc.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO dbo.ts_load (id, val) SELECT g, 'v' || g::text FROM generate_series(1, {ROWS}) g"
                if is_pg
                else f"INSERT INTO dbo.ts_load (id, val) SELECT TOP {ROWS} ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'v' FROM sys.all_objects"
            )

    cfg = WriteConfig(
        load_mode="append", primary_keys=["id"], dialect=reader.source_dialect
    )
    write_db_to_delta(sliced, ("dbo", "ts_load"), dest, cfg)
    total, lpk, full_ts = _counts(dest, conf_name)
    assert (total, lpk, full_ts) == (ROWS, ROWS, 1)

    time.sleep(1)
    write_db_to_delta(sliced, ("dbo", "ts_load"), dest, cfg)  # nothing changed
    assert _counts(dest, conf_name)[0:2] == (ROWS, ROWS)

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.ts_load SET val = 'changed' WHERE id = 7")
    time.sleep(1)
    write_db_to_delta(sliced, ("dbo", "ts_load"), dest, cfg)
    assert _counts(dest, conf_name)[0:2] == (ROWS + 1, ROWS)
