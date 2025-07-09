import odbc2deltalake
from odbc2deltalake.reader.spark_reader import SparkReader
from deltalake2db import duckdb_create_view_for_delta

try:
    import deltalake

    raise ImportError("I should not be able to import deltalake")
    exit(-1)
except ImportError:
    print("I could not import deltalake, which is good")

print("I could import ")
