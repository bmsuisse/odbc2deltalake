from .reader import DataSourceReader
from ..destination import Destination
from sqlglot.expressions import Query
from typing import Literal


class SparkReader(DataSourceReader):
    def __init__(
        self,
        spark,
        sql_config: dict[str, str] | None = None,
        linked_server_proxy: str | None = None,
        spark_format: str = "sqlserver",
    ):
        self.spark = spark
        self.sql_config = sql_config or dict()
        self.linked_server_proxy = linked_server_proxy
        self.spark_format = spark_format

    def local_register_update_view(self, delta_path: Destination, view_name: str):
        self.spark.read.format("delta").load(str(delta_path)).createOrReplaceTempView(
            view_name
        )

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

    @property
    def query_dialect(self) -> str:
        return "databricks"

    def _query(self, sql: str | Query):
        if isinstance(sql, Query):
            sql = sql.sql("tsql")
        if self.linked_server_proxy:
            assert "--" not in self.linked_server_proxy
            assert "/*" not in self.linked_server_proxy
            assert "*/" not in self.linked_server_proxy
            sql_escaped = sql.replace("'", "''")
            return f"select * from openquery([{self.linked_server_proxy}], '{ sql_escaped}')"
        return sql

    def source_sql_to_py(self, sql: str | Query) -> list[dict]:
        reader = self.spark.read.format(self.spark_format).option(
            "query", self._query(sql)
        )
        for k, v in self.sql_config.items():
            reader = reader.option(k, v)
        rows = reader.load().collect()
        return [row.asDict() for row in rows]

    def source_write_sql_to_delta(
        self, sql: str, delta_path: Destination, mode: Literal["overwrite", "append"]
    ):
        reader = self.spark.read.format(self.spark_format).option(
            "query", self._query(sql)
        )
        for k, v in self.sql_config.items():
            reader = reader.option(k, v)
        reader.load().write.format("delta").mode(mode).save(str(delta_path))
