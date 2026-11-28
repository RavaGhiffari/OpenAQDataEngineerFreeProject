import duckdb

con = duckdb.connect("data/openaq.duckdb", read_only=True)

# SHOW TABLES hanya melihat schema default `main`; tabel kita ada di schema `raw`.
tables = con.sql(
    "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2"
).fetchall()

for schema, table in tables:
    (n,) = con.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()
    # print biasa, bukan .show(): kotak Unicode .show() error di konsol Windows (cp1252)
    print(f"{schema}.{table}: {n} rows")
