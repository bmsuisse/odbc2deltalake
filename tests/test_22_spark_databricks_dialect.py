"""SparkReader.__init__ tried to detect a Databricks runtime via
`spark.conf.get("spark.home")` - but pyspark's RuntimeConfig.get(key) with no
default raises (rather than returning None) when the key is unset, per
pyspark's own source (pyspark/sql/conf.py: `if default is _NoValue: return
self._jconf.get(key)`, and Spark's underlying SQLConf.get(key) throws
NoSuchElementException for an absent key). Since "spark.home" is essentially
never actually set as a runtime SQL conf, this call always raised, was
silently swallowed by the broad `except Exception`, and `self._dialect`
therefore fell back to "spark" unconditionally - even when genuinely running
on Databricks. Databricks-specific SQL generation was consequently dead code.
"""
from unittest.mock import MagicMock


def _spark_conf_get(*args):
    # mirrors pyspark.sql.conf.RuntimeConfig.get: get(key) with no default
    # raises for an unset key (real Spark: NoSuchElementException),
    # get(key, default) returns the default instead of raising
    if len(args) == 1:
        raise KeyError(f"{args[0]} not set (simulating pyspark's NoSuchElementException)")
    return args[1]


def test_databricks_dialect_detected_via_env_var(monkeypatch):
    from odbc2deltalake.reader.spark_reader import SparkReader

    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")

    fake_spark = MagicMock()
    fake_spark.conf.get.side_effect = _spark_conf_get

    reader = SparkReader(fake_spark, spark_format="sqlserver")

    assert reader._dialect == "databricks", (
        "Databricks runtime should be detected via DATABRICKS_RUNTIME_VERSION "
        "even when spark.conf.get('spark.home') raises for an unset key"
    )


def test_plain_spark_dialect_when_not_on_databricks(monkeypatch):
    from odbc2deltalake.reader.spark_reader import SparkReader

    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    fake_spark = MagicMock()
    fake_spark.conf.get.side_effect = _spark_conf_get

    reader = SparkReader(fake_spark, spark_format="sqlserver")

    assert reader._dialect == "spark"
