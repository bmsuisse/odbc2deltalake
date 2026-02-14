# Operation Column Feature

## Overview

The `operation_column_mode` configuration option allows you to use a single `__operation` column instead of the separate `__is_deleted` and `__is_full_load` columns for tracking record operations in your delta tables.

## Configuration

### Using the New Operation Column Mode

```python
from odbc2deltalake import write_db_to_delta, WriteConfig

# Use the new operation column mode
write_config = WriteConfig(operation_column_mode="operation")
write_db_to_delta(
    source=reader,
    table_or_query=("dbo", "users"),
    destination=dest,
    write_config=write_config
)
```

### Using Legacy Mode

```python
# Use the legacy __is_deleted and __is_full_load columns
write_config = WriteConfig(operation_column_mode="is_deleted_is_full_load")
write_db_to_delta(
    source=reader,
    table_or_query=("dbo", "users"),
    destination=dest,
    write_config=write_config
)
```

### Auto-Detection Mode (Default)

```python
# Auto-detect from existing table (defaults to operation mode for new tables)
write_config = WriteConfig(operation_column_mode=None)  # or omit the parameter
write_db_to_delta(
    source=reader,
    table_or_query=("dbo", "users"),
    destination=dest,
    write_config=write_config
)
```

## Operation Column Values

When using `operation_column_mode="operation"`, the `__operation` column will contain:

- **`"reload"`**: Records from a full load operation
- **`"upsert"`**: Records that were inserted or updated
- **`"delete"`**: Soft-deleted records

## Querying Data

### With Operation Column

```sql
-- Get all active (non-deleted) records
SELECT * FROM delta_table 
WHERE __operation != 'delete'

-- Get only full load records
SELECT * FROM delta_table 
WHERE __operation = 'reload'

-- Get only delta changes (inserts/updates)
SELECT * FROM delta_table 
WHERE __operation = 'upsert'
```

### With Legacy Columns

```sql
-- Get all active (non-deleted) records
SELECT * FROM delta_table 
WHERE NOT __is_deleted

-- Get only full load records
SELECT * FROM delta_table 
WHERE __is_full_load

-- Get only delta changes
SELECT * FROM delta_table 
WHERE NOT __is_full_load AND NOT __is_deleted
```

## Migration Guide

### From Legacy to Operation Column

If you have existing tables using `__is_deleted` and `__is_full_load`:

1. The auto-detection mode will automatically use the legacy format for existing tables
2. To migrate to the new format, perform a full reload with `operation_column_mode="operation"`
3. Note: You cannot have both formats in the same table

### Backward Compatibility

- Existing code will continue to work without changes (auto-detection defaults to legacy format for existing tables)
- New tables default to the operation column mode when `operation_column_mode=None`
- You can explicitly set the mode to ensure consistent behavior

## Benefits

The new operation column mode provides:

1. **Simpler Schema**: One column instead of two
2. **Clearer Intent**: Operation type is explicit
3. **Easier Queries**: Single column to check for operation type
4. **Better Semantics**: "reload", "upsert", "delete" are more intuitive than boolean flags
