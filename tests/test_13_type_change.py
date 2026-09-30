import time
from typing import TYPE_CHECKING
import pytest
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import sqlglot as sg
import sqlglot.expressions as ex

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


def _run_type_change(
    connection: "DB_Connection",
    spark_session: "SparkSession",
    conf_name: str,
    new_type: str,
    *,
    append_only: bool = False,
):
    from odbc2deltalake import WriteConfig

    reader, dest = get_test_run_configs(
        connection, spark_session, f"dbo/user_type_change_{new_type}"
    )[conf_name]
    dialect = reader.source_dialect
    is_pg = dialect == "postgres"
    delta_col = "xmin" if is_pg else "time stamp"

    def _query(name_len_type: str):
        if is_pg:
            sql = f'select "User - iD", firstname, cast(length(firstname) as {name_len_type}) as name_len, xmin::text::bigint as xmin from dbo.user6'
        else:
            sql = f"select [User - iD], FirstName, cast(len(FirstName) as {name_len_type}) as name_len, [time stamp] from dbo.user6"
        q = sg.parse_one(sql, dialect=dialect)
        assert isinstance(q, ex.Query)
        return q

    config = WriteConfig(
        primary_keys=["User_-_iD"], delta_col=delta_col, dialect=dialect
    )
    write_db_to_delta_with_check(reader, _query("int"), dest, write_config=config)
    ops = reader.get_local_delta_ops(dest / "delta")
    if append_only:
        ops.set_properties({"delta.appendOnly": "true"})

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.user6 SET FirstName='Changed' where Age < 20")
    time.sleep(2)

    write_db_to_delta_with_check(reader, _query(new_type), dest, write_config=config)
    return reader, dest, ops


@pytest.mark.parametrize("conf_name", config_names)
def test_source_type_widening_does_not_fail_on_stale_staging(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    # the temporary delta_load tables written by the first run still have the old
    # schema and must not block the next run
    _run_type_change(connection, spark_session, conf_name, "bigint")


def test_source_type_change_int_to_string_spark(
    connection: "DB_Connection", spark_session: "SparkSession"
):
    if spark_session is None or "spark" not in config_names:
        pytest.skip("Column migration is only implemented for spark")
    reader, dest, ops = _run_type_change(
        connection, spark_session, "spark", "text", append_only=True
    )
    df = spark_session.read.format("delta").load(str(dest / "delta"))
    assert dict(df.dtypes)["name_len"] == "string"
    assert df.columns.index("name_len") == 2  # keeps its position
    assert ops.get_property("delta.appendOnly") == "true"
    assert df.where("name_len is null").count() == 0
    assert df.where("name_len = '5'").count() > 0  # old rows were cast, not lost
