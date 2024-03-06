# ODBC 2 Deltalake

This is a package that supports reading from ODBC and writing to a scd2 DeltaTable. The delta table will always have the following "system" cols:

- \_\_timestamp : The Date of the Load
- \_\_is_deleted : True for a deletion of a record
- \_\_is_full_load : True if it was a full load, meaning an implicit deletion of not delivered records

Currently, this package is focused very much on Microsoft SQL Server. But it should not be too hard to add support for other DB Systems.

This packages handles delta's based on MS-SQL's "timestamp"/"rowversion" DataType or based on Temporal Tables. Other methods will be added later on. It does detect updates to records
that happened differently, eg if a restore happened in the source.

## Usage

It's as simple as this:

```python
from odbc2deltalake import write_db_to_delta

await write_db_to_delta(
    your_onnection_string,
    (your_db_schema, your_table_name),
    Path("YOUR_TARGET_PATH")
)
```
