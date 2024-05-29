import pyodbc
import pytest
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

load_dotenv()

logger = logging.getLogger(__name__)


class DB_Connection:
    def __init__(self):
        import shutil

        if os.path.exists("tests/_data"):
            shutil.rmtree("tests/_data")
        os.makedirs("tests/_data", exist_ok=True)
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
        self.master_conn = pyodbc.connect(conn_str, autocommit=True)
        configs = ["azure", "spark", "local"]
        tc = os.getenv("ODBCLAKE_TEST_CONFIGURATION")
        if tc:
            configs = [tc]
        self.conn_str: dict[str, str] = dict()
        for cfg in configs:
            db_name = "db_to_delta_test_" + cfg
            with self.master_conn.cursor() as cursor:
                try:
                    cursor.execute(" drop DATABASE if exists " + db_name)
                    cursor.execute("CREATE DATABASE " + db_name)
                except Exception as e:
                    logger.error("Error drop creating db", exc_info=e)
            with self.master_conn.cursor() as cursor:
                cursor.execute("USE " + db_name)
            with open("tests/sqls/init.sql", encoding="utf-8-sig") as f:
                sqls = f.read().replace("\r\n", "\n").split("\nGO\n")
                for sql in sqls:
                    with self.master_conn.cursor() as cursor:
                        cursor.execute(sql)
            self.conn_str[cfg] = conn_str.replace(
                "database=master", "database=" + db_name
            ).replace("Database=master", "Database=" + db_name)
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
        return pyodbc.connect(self.conn_str[cfg_name], autocommit=True)

    def close(self):
        self.master_conn.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.master_conn.close()


@pytest.fixture(scope="session")
def spawn_sql():
    import test_server
    import os

    if os.getenv("NO_SQL_SERVER", "0") == "1":
        yield None
    else:
        sql_server = test_server.start_mssql_server()
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
