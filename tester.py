import sqlglot

with open("tests/sqls/init.sql", encoding="utf-8") as f:
    sqls = f.read().replace("\r\n", "\n").split("\nGO\n")

for sql in sqls:
    try:
        res = sqlglot.parse(sql, dialect="tsql")
        for r in res:
            print(r.sql("databricks"))
    except Exception as err:
        print("err: " + str(err))
