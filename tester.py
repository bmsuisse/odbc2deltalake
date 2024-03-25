import sqlglot

sqlglot.parse_one("select * from openjson('[]')")
