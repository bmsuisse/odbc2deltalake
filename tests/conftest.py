import pytest
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal, cast
from typing_extensions import LiteralString

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

load_dotenv()

use_postgres = os.getenv("ODBCLAKE_TEST_SOURCE_SERVER") == "postgres"
source_server: Final[Literal["mssql", "postgres"]] = (
    "mssql" if not use_postgres else "postgres"
)

logger = logging.getLogger(__name__)


def get_conn(conn_str: str):
    if source_server == "mssql":
        import pyodbc

        return pyodbc.connect(conn_str, autocommit=True)
    else:
        import adbc_driver_postgresql.dbapi

        return adbc_driver_postgresql.dbapi.connect(conn_str)


class DB_Connection:
    def __init__(self):
        import shutil

        self.source_server = source_server
        self.dialect = "tsql" if source_server == "mssql" else source_server
        if os.path.exists("tests/_data"):
            shutil.rmtree("tests/_data")
        os.makedirs("tests/_data", exist_ok=True)
        configs = ["azure", "spark", "local"]
        tc = os.getenv("ODBCLAKE_TEST_CONFIGURATION")
        if tc:
            configs = [tc]
        self.conn_str: dict[str, str] = dict()
        if source_server == "mssql":
            from odbc2deltalake.odbc_utils import build_connection_string

            conn_str = build_connection_string(
                os.getenv("ODBC_MASTER_CONN", None)
                or {
                    "server": "127.0.0.1,1444",
                    "database": "master",
                    "encrypt": "yes",
                    "TrustServerCertificate": "Yes",
                    "UID": "sa",
                    "PWD": "MyPass@word4tests",
                },
                odbc=True,
            )
            self.conn_str_master = conn_str
            for cfg in configs:
                db_name = "db_to_delta_test_" + cfg
                self.conn_str[cfg] = self.conn_str_master.replace(
                    "database=master", "database=" + db_name
                ).replace("Database=master", "database=" + db_name)
        else:
            connstr = (
                "postgresql://testuser:MyPasswortli4tests@localhost:54320/postgres"
            )
            self.conn_str_master = connstr
            for cfg in configs:
                db_name = "db_to_delta_test_" + cfg
                self.conn_str[cfg] = (
                    "postgresql://testuser:MyPasswortli4tests@localhost:54320/"
                    + db_name
                )

        master_conn = get_conn(self.conn_str_master)
        for cfg in configs:
            with master_conn.cursor() as cursor:
                try:
                    if source_server == "postgres":
                        cursor.execute("SET AUTOCOMMIT = ON")
                    cursor.execute(
                        cast(LiteralString, " drop DATABASE if exists " + db_name)
                    )
                    cursor.execute(cast(LiteralString, "CREATE DATABASE " + db_name))
                except Exception as e:
                    logger.error("Error drop creating db", exc_info=e)

            with get_conn(self.conn_str[cfg]) as con:
                with open(
                    f"tests/sqls/init_{source_server}.sql", encoding="utf-8-sig"
                ) as f:
                    sqls = f.read().replace("\r\n", "\n").split("\nGO\n")
                    for sql in sqls:
                        with con.cursor() as cursor:
                            if source_server == "postgres":
                                cursor.execute("SET AUTOCOMMIT = ON")
                            cursor.execute(cast(LiteralString, sql))

            if db_name not in self.conn_str[cfg]:
                raise ValueError("Database not created correctly")

    def __enter__(self):
        return self

    def get_jdbc_options(self, cfg_name: str):
        parts = self.conn_str[cfg_name].split(";")
        part_map = {p.split("=")[0]: p.split("=")[1] for p in parts}
        map_keys = {"UID": "user", "PWD": "password", "server": "host"}
        d = {map_keys.get(k, k): v for k, v in part_map.items()}
        d["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        return d

    def new_connection(self, cfg_name: str):
        return get_conn(self.conn_str[cfg_name])

    def close(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass


@pytest.fixture(scope="session")
def spawn_sql():
    import test_server
    import os

    if os.getenv("NO_SQL_SERVER", "0") == "1":
        yield None
    else:
        sql_server = test_server.start_server(source_server)
        yield sql_server
        if os.getenv("KEEP_SQL_SERVER", "0") == "0":  # can be handy during development
            sql_server.stop()


@pytest.fixture(scope="session")
def connection(spawn_sql):
    c = DB_Connection()
    yield c
    c.close()


@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    import test_server
    import os

    if os.getenv("ODBCLAKE_TEST_CONFIGURATION", "azure").lower() != "azure":
        yield None
        return
    if os.getenv("NO_AZURITE_DOCKER", "0") == "1":
        test_server.create_test_blobstorage()
        yield None
        return
    else:
        azurite = test_server.start_azurite()
        yield azurite
        if (
            os.getenv("KEEP_AZURITE_DOCKER", "0") == "0"
        ):  # can be handy during development
            azurite.stop()


@pytest.fixture(scope="session")
def spark_session():
    if os.getenv("NO_SPARK", "0") == "1":
        return None
    if os.getenv("ODBCLAKE_TEST_CONFIGURATION", "spark").lower() != "spark":
        return None
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    jar = str(Path("tests/jar").absolute())
    builder = (
        SparkSession.builder.appName("test_odbc2deltalake")  # type: ignore
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .config("spark.memory.fraction", 0.8)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars", str(Path("tests/jar/mssql-jdbc-12.6.1.jre11.jar").absolute())
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


@pytest.fixture(autouse=True)
def session_clear(spark_session: "SparkSession"):
    yield
    if spark_session:
        spark_session.catalog.clearCache()
