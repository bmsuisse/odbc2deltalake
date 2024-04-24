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

## Use a query as source

Instead of reading from a table/view, you can also read from a query. To use, pass a sqlglot Query instance, like so:

```python
import sqlglot as sg
from odbc2deltalake import write_db_to_delta, WriteConfig

query = sg.parse_one("select *  from dbo.[user] where age > 50", dialect="tsql") # sync only a subset
write_db_to_delta(reader, query, dest, WriteConfig(primary_keys=["user_id"]))

```

### Advanced Scenarios

#### Retrieve really used infos

You can also execute the sync by using this method, which allows you to get details about used primary keys and columns:

```python
from odbc2deltalake import make_writer

writer = make_writer(reader, query, ("dbo", "user"), WriteConfig(primary_keys=["user_id"]))
# use writer to retrieve primary keys and other infos
writer.execute() # really do it

```

#### Configuration

See class WriteConfig, of which you can pass an instance to `write_db_to_delta`

```python
@dataclass(frozen=True)
class WriteConfig:

    dialect: str = "tsql"
    """The sqlglot dialect to use for the SQL generation against the source"""

    primary_keys: list[str] | None = None
    """A list of primary keys to use for the delta load. If None, the primary keys will be determined from the source"""

    delta_col: str | None = None
    """The column to use for the delta load. If None, the column will be determined from the source. Should be mostly increasing to make load efficient"""

    load_mode: Literal[
        "overwrite",
        "append",
        "force_full",
        "append_inserts",
        "simple_delta",
        "simple_delta_check",
    ] = "append"
    """The load mode to use. Attention: overwrite will not help you build scd2, the history is in the delta table only
        append_inserts is for when you have a delta column which is strictly increasing and you want to append new rows only. No deletes of rows. might be good for logs
        simple_delta is for sources where the delta col is a datetime and you can be sure that there are no deletes or additional updates
        simple_delta_check is like simple_delta, but checks for deletes if the count does not match. Only use if you do not expect frequent deletes, as it will do simple_delta AND delta if there are deletes, which is slower than delta
    """

    data_type_map: Mapping[str, ex.DATA_TYPE] = dataclasses.field(
        default_factory=lambda: _default_type_map.copy() # defaults to some simple sql server related maps
    )
    """Set this if you want to map stuff like decimal to double before writing to delta. We recommend doing so later in ETL usually"""

    no_complex_entries_load: bool = False
    """If true, will not load 'strange updates' via OPENJSON. Use if your db does not support OPENJSON or you're fine to get some additional updates in order to reduce complexity"""

    get_target_name: Callable[[InformationSchemaColInfo], str] = dataclasses.field(
        default_factory=lambda: compat_name # defaults to removing spaces and other characters not liked by spark
    )
    """A method that returns the target name of a column. This is used to map the source column names to the target column names.
    Use if you want to apply some naming convention or avoid special characters in the target. """

```
