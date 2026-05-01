"""Tests for the __operation column feature."""
from typing import TYPE_CHECKING
import pytest
from deltalake2db import duckdb_create_view_for_delta
import duckdb
from .utils import get_test_run_configs, config_names
from odbc2deltalake import WriteConfig

if TYPE_CHECKING:
    from tests.conftest import DB_Connection
    from pyspark.sql import SparkSession


@pytest.mark.order(101)
@pytest.mark.parametrize("conf_name", config_names)
def test_operation_column_full_load(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Test full load with operation column mode."""
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_op")[
        conf_name
    ]

    # Write with operation column mode
    write_config = WriteConfig(operation_column_mode="operation")
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_op",
            use_delta_ext=conf_name == "spark",
        )
        
        # Check that __operation column exists and has 'reload' value
        operation_tuples = con.execute(
            'SELECT DISTINCT __operation from v_user_op'
        ).fetchall()
        assert operation_tuples == [("reload",)]
        
        # Check that old columns don't exist
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'v_user_op'"
        ).fetchall()
        column_names = [col[0] for col in columns]
        assert "__operation" in column_names
        assert "__is_deleted" not in column_names
        assert "__is_full_load" not in column_names


@pytest.mark.order(102)
@pytest.mark.parametrize("conf_name", config_names)
def test_operation_column_delta_load(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Test delta load with operation column mode (upsert and delete)."""
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_op2")[
        conf_name
    ]

    # First load with operation column mode
    write_config = WriteConfig(operation_column_mode="operation")
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config,
    )

    # Update data in source database
    with connection.new_connection(conf_name) as nc:
        cursor = nc.cursor()
        cursor.execute("UPDATE dbo.[user] SET FirstName='Johnny' WHERE [User - iD]=1")
        cursor.execute("DELETE FROM dbo.[user] WHERE [User - iD]=3")
        nc.commit()

    # Second load (delta)
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_op2",
            use_delta_ext=conf_name == "spark",
        )
        
        # Check operation values
        operation_tuples = con.execute(
            'SELECT DISTINCT __operation from v_user_op2 ORDER BY __operation'
        ).fetchall()
        # Should have reload, upsert, and delete
        assert set(op[0] for op in operation_tuples).issuperset({"reload", "upsert", "delete"})


@pytest.mark.order(103)
@pytest.mark.parametrize("conf_name", config_names)
def test_auto_detection_mode(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Test auto-detection of operation mode from existing table."""
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_auto")[
        conf_name
    ]

    # First load with operation mode explicitly set
    write_config = WriteConfig(operation_column_mode="operation")
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config,
    )

    # Second load with auto mode (should detect operation mode from existing table)
    write_config_auto = WriteConfig(operation_column_mode=None)
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config_auto,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_auto",
            use_delta_ext=conf_name == "spark",
        )
        
        # Verify __operation column is still used
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'v_user_auto'"
        ).fetchall()
        column_names = [col[0] for col in columns]
        assert "__operation" in column_names


@pytest.mark.order(104)
@pytest.mark.parametrize("conf_name", config_names)
def test_backward_compatibility_legacy_mode(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Test that legacy mode still works with __is_deleted and __is_full_load."""
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_legacy")[
        conf_name
    ]

    # Load with legacy mode explicitly set
    write_config = WriteConfig(operation_column_mode="is_deleted_is_full_load")
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_legacy",
            use_delta_ext=conf_name == "spark",
        )

        # Check that legacy columns exist
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'v_user_legacy'"
        ).fetchall()
        column_names = [col[0] for col in columns]
        assert "__is_deleted" in column_names
        assert "__is_full_load" in column_names
        assert "__operation" not in column_names


@pytest.mark.order(105)
@pytest.mark.parametrize("conf_name", config_names)
def test_auto_detection_legacy_mode(
    connection: "DB_Connection", spark_session: "SparkSession", conf_name: str
):
    """Test auto-detection stays in legacy mode for existing legacy tables."""
    from odbc2deltalake import write_db_to_delta

    reader, dest = get_test_run_configs(connection, spark_session, "dbo/user_legacy_auto")[
        conf_name
    ]

    # First load with legacy mode explicitly set
    write_config = WriteConfig(operation_column_mode="is_deleted_is_full_load")
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config,
    )

    # Second load with auto mode (should detect and stay in legacy mode)
    write_config_auto = WriteConfig(operation_column_mode=None)
    write_db_to_delta(
        reader,
        ("dbo", "user"),
        dest,
        write_config=write_config_auto,
    )

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con,
            (dest / "delta").as_delta_table(),
            "v_user_legacy_auto",
            use_delta_ext=conf_name == "spark",
        )

        # Verify legacy columns are still used
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'v_user_legacy_auto'"
        ).fetchall()
        column_names = [col[0] for col in columns]
        assert "__is_deleted" in column_names
        assert "__is_full_load" in column_names
        # __operation should not be added by auto-detection
        assert "__operation" not in column_names

