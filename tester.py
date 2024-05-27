import sqlglot
import sqlglot.expressions as exp

print(sqlglot.parse_one("select X'1A2B'", dialect="spark").sql("databricks"))
