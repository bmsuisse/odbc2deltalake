import time
from typing import TYPE_CHECKING
import pytest
from .utils import write_db_to_delta_with_check, config_names, get_test_run_configs
import sqlglot as sg
import sqlglot.expressions as ex

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.parametrize("conf_name", config_names)
def test_source_type_change_does_not_fail_on_stale_staging(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    from odbc2deltalake import WriteConfig

    reader, dest = get_test_run_configs(
        connection, spark_session, "dbo/user_type_change"
    )[conf_name]
    dialect = reader.source_dialect
    is_pg = dialect == "postgres"
    delta_col = "xmin" if is_pg else "time stamp"

    def _query(int_type: str):
        if is_pg:
            sql = f'select "User - iD", firstname, cast(length(firstname) as {int_type}) as name_len, xmin::text::bigint as xmin from dbo.user6'
        else:
            sql = f"select [User - iD], FirstName, cast(len(FirstName) as {int_type}) as name_len, [time stamp] from dbo.user6"
        q = sg.parse_one(sql, dialect=dialect)
        assert isinstance(q, ex.Query)
        return q

    config = WriteConfig(
        primary_keys=["User_-_iD"], delta_col=delta_col, dialect=dialect
    )
    write_db_to_delta_with_check(reader, _query("int"), dest, write_config=config)

    with connection.new_connection(conf_name) as nc:
        with nc.cursor() as cursor:
            cursor.execute("UPDATE dbo.user6 SET FirstName='Changed' where Age < 20")
    time.sleep(2)

    # the datatype of name_len changed: the temporary delta_load tables written by the
    # first run still have the old schema and must not block the next run
    write_db_to_delta_with_check(reader, _query("bigint"), dest, write_config=config)
