import logging
import os
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Final, Literal, cast

import pytest
from dotenv import load_dotenv
from typing_extensions import LiteralString

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

load_dotenv(override=True)

use_postgres = os.getenv("ODBCLAKE_TEST_SOURCE_SERVER") == "postgres"
source_server: Final[Literal["mssql", "postgres"]] = "mssql" if not use_postgres else "postgres"

logger = logging.getLogger(__name__)


def get_conn(conn_str: str, *, autocommit: bool, attempt=0):
    try:
        if source_server == "mssql":
            import pyodbc

            return pyodbc.connect(conn_str, autocommit=autocommit)
        import adbc_driver_postgresql.dbapi

        return adbc_driver_postgresql.dbapi.connect(conn_str, autocommit=autocommit)
    except Exception as e:
        if attempt < 3:
            sleep(3)  # wait a bit before retrying
            logger.warning("Connection failed, retrying... Attempt %d: %s", attempt + 1, e)
            return get_conn(conn_str, autocommit=autocommit, attempt=attempt + 1)
        raise RuntimeError(f"Failed to connect after 3 attempts: {conn_str}\r\n {e}") from e


class DBConnection:
    def __init__(self):
        import shutil

        self.source_server = source_server
        self.dialect = "tsql" if source_server == "mssql" else source_server
        if Path("tests/_data").exists():
            shutil.rmtree("tests/_data")
        Path("tests/_data").mkdir(parents=True, exist_ok=True)
        configs = ["azure", "spark", "local"]
        tc = os.getenv("ODBCLAKE_TEST_CONFIGURATION")
        if tc:
            configs = [tc]
        self.conn_str: dict[str, str] = {}
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
                self.conn_str[cfg] = self.conn_str_master.replace("database=master", "database=" + db_name).replace(
                    "Database=master", "database=" + db_name
                )
        else:
            # base url without database, e.g. postgresql://user:pw@localhost:5432
            pg_base = os.getenv(
                "ODBCLAKE_TEST_PG_URL",
                "postgresql://testuser:MyPasswortli4tests@localhost:54320",
            ).rstrip("/")
            self.conn_str_master = pg_base + "/postgres"
            for cfg in configs:
                db_name = "db_to_delta_test_" + cfg
                self.conn_str[cfg] = pg_base + "/" + db_name

        master_conn = get_conn(self.conn_str_master, autocommit=True)
        for cfg in configs:
            with master_conn.cursor() as cursor:
                try:
                    cursor.execute(cast(LiteralString, " drop DATABASE if exists " + db_name))
                    cursor.execute(cast(LiteralString, "CREATE DATABASE " + db_name))
                except Exception as e:
                    logger.error("Error drop creating db", exc_info=e)

            with (
                get_conn(self.conn_str[cfg], autocommit=True) as con,
                Path(f"tests/sqls/init_{source_server}.sql").open(encoding="utf-8-sig") as f,
            ):
                sqls = f.read().replace("\r\n", "\n").split("\nGO\n")
                for sql in sqls:
                    with con.cursor() as cursor:
                        if hasattr(cursor, "executescript"):  # type: ignore
                            # for postgres
                            cursor.executescript(sql)
                        else:
                            cursor.execute(cast(LiteralString, sql))

            if db_name not in self.conn_str[cfg]:
                raise ValueError("Database not created correctly")

    def __enter__(self):
        return self

    def get_jdbc_options(self, cfg_name: str):
        if self.conn_str[cfg_name].startswith("postgresql://"):
            # Postgres connection string
            parts = self.conn_str[cfg_name].split("/")
            user_pass, host_port = parts[2].split("@")
            user, password = user_pass.split(":")
            host, port = host_port.split(":")
            return {
                "user": user,
                "password": password,
                "host": host,
                "port": port,
                "driver": "org.postgresql.Driver",
                "database": parts[3],
            }
        parts = self.conn_str[cfg_name].split(";")
        part_map = {p.split("=")[0]: p.split("=")[1] for p in parts}
        map_keys = {"UID": "user", "PWD": "password", "server": "host"}
        d = {map_keys.get(k, k): v for k, v in part_map.items()}
        d["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        return d

    def new_connection(self, cfg_name: str):
        return get_conn(self.conn_str[cfg_name], autocommit=True)

    def close(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass


@pytest.fixture(scope="session")
def spawn_sql():
    import os

    import test_server

    if os.getenv("NO_SQL_SERVER", "0") == "1":
        yield None
    else:
        sql_server = test_server.start_server(source_server)
        yield sql_server
        if os.getenv("KEEP_SQL_SERVER", "0") == "0":  # can be handy during development
            sql_server.stop()


@pytest.fixture(scope="session")
def connection(spawn_sql):
    c = DBConnection()
    yield c
    c.close()


@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    import os

    import test_server

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
        if os.getenv("KEEP_AZURITE_DOCKER", "0") == "0":  # can be handy during development
            azurite.stop()


@pytest.fixture(scope="session")
def spark_session():
    if os.getenv("NO_SPARK", "0") == "1":
        return None
    if os.getenv("ODBCLAKE_TEST_CONFIGURATION", "spark").lower() != "spark":
        return None
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

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
    )
    if source_server == "mssql":
        builder = builder.config("spark.jars", str(Path("tests/jar/mssql-jdbc-12.6.1.jre11.jar").absolute()))
    else:
        builder = builder.config("spark.jars", str(Path("tests/jar/postgresql-42.7.7.jar").absolute()))

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(autouse=True)
def session_clear(spark_session: "SparkSession"):
    yield
    if spark_session:
        spark_session.catalog.clearCache()
