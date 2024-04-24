import sqlglot.expressions as ex
import sqlglot
from pydantic.v1 import BaseModel, ConfigDict

print(  # transpiled type timestamp as is, should be binary
    sqlglot.parse_one("create table a (b timestamp)", dialect="tsql").sql("databricks")
)

print(  # prints ROWVERSION instead of BINARY
    sqlglot.parse_one("create table a (b rowversion)", dialect="tsql").sql("databricks")
)

print(  # good, this works! databricks timestamp is datetime2 in ms sql
    sqlglot.parse_one("create table a (b timestamp)", dialect="databricks").sql("tsql")
)

print(repr(ex.DataType.build("timestamp", dialect="tsql")))
print(
    sqlglot.parse_one("create table a (b timestamp)", dialect="tsql").sql(
        "tsql"
    )  # prints datetime2
)


class TestClass(BaseModel):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    class Config:
        arbitrary_types_allowed = True

    dt: ex.DataType


for t in ["varchar", "nchar", "nvarchar", "char", "text", "ntext"]:
    print(ex.DataType.build(t, dialect="tsql").this)
    assert (
        ex.DataType.build(t, dialect="tsql").this in ex.DataType.TEXT_TYPES
    ), f"{t} is not a string type"
