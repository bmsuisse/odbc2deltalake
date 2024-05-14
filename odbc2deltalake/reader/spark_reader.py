from .reader import DataSourceReader, DeltaOps
from ..destination import Destination
from sqlglot.expressions import Query, DataType
from typing import Literal, TYPE_CHECKING, Callable, Optional, Union
import os

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from odbc2deltalake.metadata import InformationSchemaColInfo


class SparkDeltaOps(DeltaOps):
    def __init__(self, dest: Destination, spark):
        from delta.tables import DeltaTable

        self.dest = dest
        self.spark = spark
        self.table = DeltaTable.forPath(spark, str(dest))

    def version(self) -> int:
        return self.table.history(1).select("version").collect()[0].version

    def vacuum(self, retention_hours: Union[int, None] = None):
        self.table.vacuum(retention_hours)

    def restore(self, target: int):
        self.table.restoreToVersion(target)

    def set_properties(self, props: dict[str, str]):
        def _escape(s: str):
            return s.replace("'", "''")

        prop_str = ", ".join(
            [f"'{_escape(k)}' = '{_escape(v)}'" for k, v in props.items()]
        )
        self.spark.sql(
            f"ALTER TABLE delta.`{str(self.dest)}` SET TBLPROPERTIES {prop_str}"
        )

    def get_property(self, key: str) -> Optional[str]:
        from pyspark.sql.functions import col, lit

        res = (
            self.spark.sql(f"show tblproperties delta.`{str(self.dest)}`")
            .where(col("key") == lit(key))
            .select("value")
            .collect()
        )
        if res:
            return res[0].value
        return None


class SparkReader(DataSourceReader):
    def __init__(
        self,
        spark: "SparkSession",
        sql_config: Optional[dict[str, str]] = None,
        linked_server_proxy: Union[str, None] = None,
        spark_format: str = "sqlserver",
        jdbc=False,
        transformation_hook: Optional[Callable[["DataFrame", str], "DataFrame"]] = None,
    ):
        self.spark = spark
        self.sql_config = sql_config or dict()
        self.linked_server_proxy = linked_server_proxy
        self.spark_format = spark_format
        self.jdbc = jdbc
        self.transformation_hook: Callable[["DataFrame", str], "DataFrame"] = (
            transformation_hook or (lambda d, _: d)
        )

        self._dialect = (
            "databricks"
            if "/databricks" in (spark.conf.get("spark.home") or "")
            else "spark"
        )

    def local_register_update_view(
        self,
        delta_path: Destination,
        view_name: str,
        *,
        version: Union[int, None] = None,
    ):
        read = self.spark.read.format("delta")
        if version is not None:
            read = read.option("versionAsOf", version)
        read.load(str(delta_path)).createOrReplaceTempView(view_name)

    def local_register_view(self, sql: Query, view_name: str):
        self.spark.sql(
            f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS {sql.sql('databricks')}"
        )

    def local_execute_sql_to_py(self, sql: Query) -> list[dict]:
        spark_rows = self.spark.sql(sql.sql("databricks")).collect()
        return [row.asDict() for row in spark_rows]

    def local_execute_sql_to_delta(
        self, sql: Query, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        self.spark.sql(sql.sql("databricks")).write.format("delta").option(
            "mergeSchema" if mode == "append" else "overwriteSchema", "true"
        ).mode(mode).save(str(delta_path))

    def local_pylist_to_delta(
        self,
        pylist: list[dict],
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        dummy_record: Union[dict, None] = None,
    ):
        schema = (
            self.spark.createDataFrame([dummy_record]).schema if dummy_record else None  # type: ignore
        )
        df = self.spark.createDataFrame(pylist, schema=schema)  # type: ignore
        df.write.format("delta").option(
            "mergeSchema" if mode == "append" else "overwriteSchema", "true"
        ).mode(mode).save(str(delta_path))

    @property
    def query_dialect(self) -> str:
        return self._dialect

    @property
    def supports_proc_exec(self):
        return False

    def _query(self, sql: Union[str, Query]):
        if isinstance(sql, Query):
            sql = sql.sql("tsql")
        if self.linked_server_proxy:
            assert "--" not in self.linked_server_proxy
            assert "/*" not in self.linked_server_proxy
            assert "*/" not in self.linked_server_proxy
            sql_escaped = sql.replace("'", "''")
            return f"select * from openquery([{self.linked_server_proxy}], '{ sql_escaped}')"
        return sql

    def source_schema_limit_one(self, sql: Query) -> "list[InformationSchemaColInfo]":
        from ..metadata import InformationSchemaColInfo

        def _sql_t(t: str):
            if t == "timestamp":
                return "datetime2"
            return t

        limit_query = sql.limit(0)
        reader = self._reader(limit_query)
        df = self.transformation_hook(reader.load(), "metadata")
        return [
            InformationSchemaColInfo(
                column_name=col.name,
                data_type=DataType.build(col.dataType.simpleString(), dialect="spark"),
                is_nullable=col.nullable,
            )
            for col in df.schema.fields
        ]

    def source_sql_to_py(self, sql: Union[str, Query]) -> list[dict]:
        reader = self._reader(sql)
        rows = self.transformation_hook(reader.load(), "source2py").collect()
        return [row.asDict() for row in rows]

    def local_delta_table_exists(
        self, delta_path: Destination, extended_check=False
    ) -> bool:
        from delta import DeltaTable

        if delta_path.exists() and DeltaTable.isDeltaTable(self.spark, str(delta_path)):
            if extended_check:
                return (
                    len(
                        self.spark.sql(
                            f"select * from delta.`{str(delta_path)}` limit 0"
                        ).columns
                    )
                    > 0
                )
            return True
        else:
            return False

    def _reader(self, sql: Union[str, Query]):
        if self.jdbc:
            options = {}
            jdbcUrl = f"jdbc:{self.spark_format}://"
            if "host" in self.sql_config:
                jdbcUrl += self.sql_config["host"].replace(",", ":")
            if "server" in self.sql_config:
                jdbcUrl += self.sql_config["server"].replace(",", ":")
            if "port" in self.sql_config:
                jdbcUrl += ":" + str(self.sql_config["port"])

            for key, value in self.sql_config.items():
                if key.lower() in ["host", "port", "server"]:
                    continue
                if key.lower() in [
                    "encrypt",
                    "TrustServerCertificate".lower(),
                    "integratedSecurity".lower(),
                ]:
                    enc_vl = value
                    if enc_vl.lower() == "yes":
                        enc_vl = "true"
                    elif enc_vl.lower() == "no":
                        enc_vl = "false"
                    assert enc_vl in ["true", "false"]
                    jdbcUrl += f";{key}=" + enc_vl
                elif key.lower() == "database":
                    jdbcUrl += ";databaseName=" + value
                else:
                    options[key] = value
            print(jdbcUrl)
            reader = self.spark.read.format("jdbc").option("url", jdbcUrl)
        else:
            options = self.sql_config
            reader = self.spark.read.format(self.spark_format)
        reader = reader.option("query", self._query(sql))
        for k, v in options.items():
            reader = reader.option(k, v)
        return reader

    def source_write_sql_to_delta(
        self, sql: str, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        reader = self._reader(sql)
        self.transformation_hook(reader.load(), "sql2delta").write.format(
            "delta"
        ).option("mergeSchema" if mode == "append" else "overwriteSchema", "true").mode(
            mode
        ).save(str(delta_path))

    def get_local_delta_ops(self, delta_path: Destination) -> DeltaOps:
        return SparkDeltaOps(delta_path, self.spark)
