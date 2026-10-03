"""Generic transform runner: reads one .sql file from scripts/transform/ and
executes it against the warehouse. Every transform task is a one-line call
to run_sql_file() with a different filename -- the business logic lives in
the .sql file, not in the DAG."""
import os

import duckdb

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "/opt/airflow/warehouse/indrive.duckdb")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/airflow/scripts/transform")


def run_sql_file(filename):
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute(sql)
    con.close()
    print(f"[run_sql] executed {filename}")
