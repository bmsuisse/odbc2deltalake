import sqlglot

print(sqlglot.parse_one("select X'1A2B'", dialect="spark").sql("databricks"))
