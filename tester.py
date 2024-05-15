import sqlglot.expressions as ex
import sqlglot

print(
    sqlglot.parse_one(
        "SELECT cast(1 as float)",
        dialect="tsql",
    ).sql("spark")
)
