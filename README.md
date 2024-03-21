# ODBC 2 Deltalake

This is a package that supports reading from ODBC and writing to a scd2 DeltaTable. The delta table will always have the following "system" cols:

- \_\_timestamp : The Date of the Load
- \_\_is_deleted : True for a deletion of a record
- \_\_is_full_load : True if it was a full load, meaning an implicit deletion of not delivered records

Currently, this package is focused very much on Microsoft SQL Server. But it should not be too hard to add support for other DB Systems.

This packages handles delta's based on MS-SQL's "timestamp"/"rowversion" DataType or based on Temporal Tables. Other methods will be added later on. It does detect updates to records
that happened differently, eg if a restore happened in the source.

## Usage

First, pip install it:
`pip install odbc2deltalake[local,local_azure]`

It's as simple as this:

```python
from odbc2deltalake import write_db_to_delta

write_db_to_delta(
    your_odbc_connection_string,
    (your_db_schema, your_table_name),
    Path("YOUR_TARGET_PATH")
)
```

### Usage with Databricks

You can also use databricks instead of the default delta-rs/duckdb implementation.
In that case you can do `pip install odbc2deltalake` without any dependencies other than Databricks Spark/DBUtils

```python
from odbc2deltalake.destination.databricks import DatabricksDestination
from odbc2deltalake.reader.spark_reader import SparkReader
from odbc2deltalake import write_db_to_delta


dest = DatabricksDestination(dbutils=dbutils,container="raw", account="some_account", path="raw/super_path", schema="abfss")
reader = SparkReader(spark=spark, sql_config={ # see https://docs.databricks.com/en/connect/external-systems/sql-server.html
    "host": "some_host",
    "port": "1433",
    "user": "some_user",
    "encrypt": "true",
    "password": "what_ever",
    "database": "mypage"
}, spark_format="sqlserver")

write_db_to_delta(reader, ("dbo", "user"), dest)
```
